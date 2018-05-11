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
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{LatLng, Sinusoidal}
import geotrellis.raster._
import geotrellis.raster.io.geotiff
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import spray.json.DefaultJsonProtocol._

/**
 * Visually inspect against https://neo.sci.gsfc.nasa.gov/view.php
 *
 * Run with something like
 * {{
 * spark-submit  --class astraea.spark.rasterframes.workshop.GlobalNDVI ./workshop-assembly-0.6.2-SNAPSHOT.jar s3://somebucket/someplace
 * }}
 *
 * @since 5/6/18
 */
object GlobalNDVI extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if(args.isEmpty) {
      throw new IllegalArgumentException("First argument should be an output path.")
    }

    implicit val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
      .withRasterFrames

    logger.info("GlobalNDVI initialized with args: " + args.mkString(" "))

    import spark.implicits._

    val baseDir = args(0)

    val numPartitions = 400
    val TOI = LocalDate.of(2017, 6, 7)

    val stamp = args.lift(1).getOrElse(
      System.currentTimeMillis().toString
    )

    val tilesFile = baseDir + s"/modis-nir-red-$TOI.parquet"
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(URI.create(tilesFile), conf)
    val build = args.contains("-b")

    val joined = if (build) {
      time("prepration") {
        val catalog = spark.read
          .format(MODISCatalogDataSource.NAME)
          .option("start", TOI.toString)
          .option("end", TOI.toString)
          .load()
        catalog.printSchema

        val scenes = catalog
          .where($"acquisitionDate".as[Date] at TOI)
          .repartition(numPartitions, $"granuleId")

        logger.info(s"Loading ${scenes.count} x 2 granules...")

        val b01 = scenes
          .select($"download_url", download_tiles(modis_band_url("B01")))
          .withColumnRenamed("B01_spatial_key", "spatial_key")

        val b02 = scenes
          .select($"download_url", download_tiles(modis_band_url("B02")))
          .withColumnRenamed("B02_spatial_key", "spatial_key")

        val joined = b01.join(b02, Seq("download_url", "spatial_key")).cache
        joined.write.parquet(tilesFile)
        joined
      }
    }
    else {
      spark.read.parquet(tilesFile).repartition(numPartitions)
    }

    time("analysis") {

      val start = System.currentTimeMillis()

      val ndvi = udf((b2: Tile, b1: Tile) ⇒ {
        val nir = b2.convert(FloatConstantNoDataCellType)
        val red = b1.convert(FloatConstantNoDataCellType)
        (nir - red) / (nir + red)
      })

      val withNDVI = joined
        .withColumn("ndvi", ndvi($"B02_tile", $"B01_tile"))
      withNDVI.printSchema()

      val hist = withNDVI.select(aggHistogram($"B01_tile"), aggHistogram($"B02_tile"), aggHistogram($"ndvi")).cache()
      hist.toDF("B01", "B02", "NDVI")
        .write.mode(SaveMode.Overwrite).parquet(baseDir + s"/histograms-$stamp.parquet")

      val hr = hist.first()
      logger.info(s"B01 Summary:\n${hr._1.asciiStats}\n----\n${hr._1.asciiHistogram(128)}\n")
      logger.info(s"B02 Summary:\n${hr._2.asciiStats}\n----\n${hr._2.asciiHistogram(128)}\n")
      logger.info(s"NDVI Summary:\n${hr._3.asciiStats}\n----\n${hr._3.asciiHistogram(128)}\n")

      val ndviStats = hist.first()._3.stats
      val zscoreRange = udf((t: Tile) ⇒ {
        val mean = ndviStats.mean
        val stddev = math.sqrt(ndviStats.variance)
        t.mapDouble(c ⇒ {
          // This filters out the extreme NDVI values, which are likely from quality issues in one of the bands
          if(math.abs(c) == 1) doubleNODATA
          else (c - mean) / stddev
        }).findMinMaxDouble
      })

      val scored = withNDVI
        .withColumn("zscores", zscoreRange($"ndvi"))
        .select($"B01_extent" as "extent", $"ndvi", $"zscores._1" as "zscoreMin", $"zscores._2" as "zscoreMax")
        .na.drop
        .orderBy(desc("zscoreMax"))
        .as[(Extent, Tile, Double, Double)]
        .cache()

      scored.write.mode(SaveMode.Overwrite).parquet(baseDir + s"/scored-global-ndvi-$stamp.parquet")

      scored.limit(10).collect.zipWithIndex.foreach { case ((extent, tile, min, max), idx) ⇒
        geotiff.GeoTiff(tile + 1, extent, Sinusoidal).write(s"tile-$idx-$max.tiff")
      }

      val geoms = scored.limit(10)
        .select($"extent".as[Extent], $"zscoreMax".as[Double])
        .map { case (extent, zscoreMax) ⇒
          Feature(extent.toPolygon().reproject(Sinusoidal, LatLng), Map("zscoreMax" -> zscoreMax))
        }
        .collect

      val end = System.currentTimeMillis()

      val results = JsonFeatureCollection(geoms).toJson.prettyPrint

      {
        val out = fs.create(new Path(baseDir + s"/zscored-extents-$stamp.json"))
        out.writeUTF(results)
        out.close()
      }

       {
        val out = fs.create(new Path(baseDir + s"/analysis-duration-$stamp.txt"))
        out.writeUTF(f"${(end - start)*1e-3}%.4fs")
        out.close()
      }

      spark.stop()
    }
  }
}
