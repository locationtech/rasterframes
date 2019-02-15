/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package examples

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.ml.TileExploder
import geotrellis.raster.ByteConstantNoDataCellType
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.render.{ColorRamps, IndexedColorMap}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._

object Clustering extends App {

  // Utility for reading imagery from our test data set
  def readTiff(name: String) = GeoTiffReader.readSingleband(getClass.getResource(s"/$name").getPath)

  implicit val spark = SparkSession.builder().master("local[*]").appName(getClass.getName).getOrCreate().withRasterFrames

  import spark.implicits._

  // The first step is to load multiple bands of imagery and construct
  // a single RasterFrame from them.
  val filenamePattern = "L8-B%d-Elkton-VA.tiff"
  val bandNumbers = 1 to 7
  val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray

  // For each identified band, load the associated image file
  val joinedRF = bandNumbers
    .map { b ⇒ (b, filenamePattern.format(b)) }
    .map { case (b,f) ⇒ (b, readTiff(f)) }
    .map { case (b, t) ⇒ t.projectedRaster.toRF(s"band_$b") }
    .reduce(_ spatialJoin _)

  // We should see a single spatial_key column along with 4 columns of tiles.
  joinedRF.printSchema()

  // SparkML requires that each observation be in its own row, and those
  // observations be packed into a single `Vector`. The first step is to
  // "explode" the tiles into a single row per cell/pixel
  val exploder = new TileExploder()

  // To "vectorize" the the band columns we use the SparkML `VectorAssembler`
  val assembler = new VectorAssembler()
    .setInputCols(bandColNames)
    .setOutputCol("features")

  // Configure our clustering algorithm
  val k = 5
  val kmeans = new KMeans().setK(k)

  // Combine the two stages
  val pipeline = new Pipeline().setStages(Array(exploder, assembler, kmeans))

  // Compute clusters
  val model = pipeline.fit(joinedRF)

  // Run the data through the model to assign cluster IDs to each
  val clustered = model.transform(joinedRF)
  clustered.show(8)

  // If we want to inspect the model statistics, the SparkML API requires us to go
  // through this unfortunate contortion:
  val clusterResults = model.stages.collect{ case km: KMeansModel ⇒ km}.head

  // Compute sum of squared distances of points to their nearest center
  val metric = clusterResults.computeCost(clustered)
  println("Within set sum of squared errors: " + metric)

  val tlm = joinedRF.tileLayerMetadata.left.get

  val retiled = clustered.groupBy($"spatial_key").agg(
    assemble_tile(
      $"column_index", $"row_index", $"prediction",
      tlm.tileCols, tlm.tileRows, ByteConstantNoDataCellType)
  )

  val rf = retiled.asRF($"spatial_key", tlm)

  val raster = rf.toRaster($"prediction", 186, 169)

  val clusterColors = IndexedColorMap.fromColorMap(
    ColorRamps.Viridis.toColorMap((0 until k).toArray)
  )

  raster.tile.renderPng(clusterColors).write("clustered.png")

  spark.stop()
}
