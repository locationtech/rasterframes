/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.workshop

import java.net.URI
import java.sql.Date
import java.time.LocalDate

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.experimental.datasource.awspds._
import astraea.spark.rasterframes.ml.TileExploder
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
 * Visually inspect against https://neo.sci.gsfc.nasa.gov/view.php
 *
 * Run with something like
 * {{
 * spark-submit  --class astraea.spark.rasterframes.workshop.Clustering ./workshop-assembly-0.6.2-SNAPSHOT.jar s3://somebucket/someplace
 * }}
 *
 * @since 5/6/18
 */
object Clustering extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if(args.isEmpty) {
      throw new IllegalArgumentException("First argument should be an output path.")
    }

    implicit val spark = SparkSession.builder()
      .appName(getClass.getName)
      .getOrCreate()
      .withRasterFrames

    logger.info("GlobalNDVI initialized with args: " + args.mkString(" "))

    import spark.implicits._

    val baseDir = args(0)

    val numPartitions = 400
    val TOI = LocalDate.of(2017, 6, 7)

    val stamp = args.lift(1).getOrElse(
      System.currentTimeMillis().toString
    )

    val tilesFile = baseDir + s"/modis-b1-b2-b3-b4-$TOI.parquet"
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(URI.create(tilesFile), conf)
    val exists = fs.exists(new Path(tilesFile))

    val joined = if (!exists) {
      time("prepration") {
        val catalog = spark.read
          .format(MODISCatalogDataSource.NAME)
          .option("start", TOI.toString)
          .option("end", TOI.toString)
          .load()

        val scenes = catalog
          .where($"acquisitionDate".as[Date] at TOI)
          .repartition(numPartitions, $"granuleId")
          .limit(2)

        logger.info(s"Loading ${scenes.count} x 4 granules...")

        def fetch(band: String) =
          scenes
            .select($"download_url", download_tiles(modis_band_url(band)))
            .withColumnRenamed(s"${band}_spatial_key", "spatial_key")
            .withColumnRenamed(s"${band}_extent", "extent")
            .drop(s"${band}_metadata")

        val bands = Seq(fetch("B01"), fetch("B02").drop("extent"), fetch("B03").drop("extent"), fetch("B04").drop("extent"))

        val joinCols = Seq("download_url", "spatial_key")
        val joined = bands.reduce(_.join(_, joinCols))
          .cache
        joined.write.parquet(tilesFile)
        joined
      }
    }
    else {
      spark.read.parquet(tilesFile).repartition(numPartitions)
    }

    val start = System.currentTimeMillis()
    val model = time("train") {
      joined.printSchema
      val bandColNames = joined.tileColumns.map(_.columnName).toArray
      val exploder = new TileExploder()

      val assembler = new VectorAssembler().
        setInputCols(bandColNames).
        setOutputCol("features")

      val k = 5
      val kmeans = new KMeans().setK(k)
      val pipeline = new Pipeline().setStages(Array(exploder, assembler, kmeans))

      pipeline.fit(joined)
    }

    time("score") {
      model.stages.foreach(println)
      model.stages.collect {
        case kmeans: KMeansModel ⇒
          val centers = kmeans.clusterCenters
          centers.zipWithIndex.foreach(p => logger.info(s"Cluster ${p._2}: ${p._1}"))
      }

      val clustered = model.transform(joined)
      clustered.write.parquet(baseDir + s"/clustered-$stamp.parquet")
    }

    val end = System.currentTimeMillis()

    {
      val out = fs.create(new Path(baseDir + s"/clustering-analysis-duration-$stamp.txt"))
      out.writeUTF(f"${(end - start)*1e-3}%.4fs")
      out.close()
    }

    spark.stop()
  }
}
