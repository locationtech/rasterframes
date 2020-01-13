/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package examples

import geotrellis.raster._
import geotrellis.raster.render.{ColorRamp, ColorRamps, Png}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.ml.{NoDataFilter, TileExploder}


object ClassificationRasterSource extends App {

  //  // Utility for reading imagery from our test data set
  def href(name: String) =  "https://raw.githubusercontent.com/locationtech/rasterframes/develop/core/src/test/resources/" + name

  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withRasterFrames

  import spark.implicits._

  // The first step is to load multiple bands of imagery and construct
  // a single RasterFrame from them.
  val filenamePattern = "L8-%s-Elkton-VA.tiff"
  val bandNumbers = 2 to 7
  val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray
  val bandSrcs = bandNumbers.map(n => filenamePattern.format("B" + n)).map(href)
  val labelSrc =  href(filenamePattern.format("Labels"))
  val tileSize = 128

  val catalog = s"${bandColNames.mkString(",")},target\n${bandSrcs.mkString(",")}, $labelSrc"


  // For each identified band, load the associated image file
  val abt = spark.read.raster.fromCSV(catalog, bandColNames :+ "target": _*).load()
    .withColumn("crs", rf_crs($"band_4"))
    .withColumn("extent", rf_extent($"band_4"))

  // We should see a single spatial_key column along with 4 columns of tiles.
  abt.printSchema()

  // Similarly pull in the target label data.
  val targetCol = "target"

  // Take a peek at what kind of label data we have to work with.
  abt.select(rf_agg_stats(abt(targetCol))).show

  // SparkML requires that each observation be in its own row, and those
  // observations be packed into a single `Vector`. The first step is to
  // "explode" the tiles into a single row per cell/pixel
  val exploder = new TileExploder()

  val noDataFilter = new NoDataFilter()
    .setInputCols(bandColNames :+ targetCol)

  // To "vectorize" the the band columns we use the SparkML `VectorAssembler`
  val assembler = new VectorAssembler()
    .setInputCols(bandColNames)
    .setOutputCol("features")

  // Using a decision tree for classification
  val classifier = new DecisionTreeClassifier()
    .setLabelCol(targetCol)
    .setFeaturesCol(assembler.getOutputCol)

  // Assemble the model pipeline
  val pipeline = new Pipeline()
    .setStages(Array(exploder, noDataFilter, assembler, classifier))

  // Configure how we're going to evaluate our model's performance.
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol(targetCol)
    .setPredictionCol("prediction")
    .setMetricName("f1")

  val model =  pipeline.fit(abt)

  // Score the original data set, including cells
  // without target values.
  val scored = model.transform(abt.drop("target"))

  // Add up class membership results
  scored.groupBy($"prediction" as "class").count().show

  scored.show(10)

  val retiled: DataFrame = scored.groupBy($"crs", $"extent").agg(
    rf_assemble_tile(
      $"column_index", $"row_index", $"prediction",
      186, 169, IntConstantNoDataCellType
    )
  )

  val clusterColors = ColorRamp(
    ColorRamps.Viridis.toColorMap((0 until 3).toArray).colors
  )

  val pngBytes = retiled.select(rf_render_png($"prediction", clusterColors)).first

  Png(pngBytes).write("classified.png")

  spark.stop()
}