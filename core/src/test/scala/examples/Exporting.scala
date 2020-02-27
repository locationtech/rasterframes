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
import java.nio.file.Files

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark.store.LayerWriter
import geotrellis.store.LayerId
import geotrellis.store.index.ZCurveKeyIndexMethod
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes._

object Exporting extends App {


  implicit val spark = SparkSession.builder().
    master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
  val rf = scene.projectedRaster.toLayer(128, 128).cache()

  //  While the goal of RasterFrames is to make it as easy as possible to do your geospatial analysis with a single
  //    construct, it is helpful to be able to transform it into other representations for various use cases.

  // ## Converting to Array

  //  The cell values within a `Tile` are encoded internally as an array. There may be use cases
  //  where the additional context provided by the `Tile` construct is no longer needed and one would
  //    prefer to work with the underlying array data.
  //
  //  The @scaladoc[`tile_to_array`][tile_to_array] column function requires a type parameter to indicate the array element
  //  type you would like used. The following types may be used: `Int`, `Double`, `Byte`, `Short`, `Float`

  val withArrays = rf.withColumn("tileData", rf_tile_to_array_int($"tile")).drop("tile")
  withArrays.show(5, 40)

  //  You can convert the data back to an array, but you have to specify the target tile dimensions.

  val tileBack = withArrays.withColumn("tileAgain", rf_array_to_tile($"tileData", 128, 128))
  tileBack.drop("tileData").show(5, 40)

  //  Note that the created tile will not have a `NoData` value associated with it. Here's how you can do that:

  val tileBackAgain = withArrays.withColumn("tileAgain", rf_with_no_data(rf_array_to_tile($"tileData", 128, 128), 3))
  tileBackAgain.drop("tileData").show(5, 50)

  //  ## Writing to Parquet
  //
  //  It is often useful to write Spark results in a form that is easily reloaded for subsequent analysis.
  //  The [Parquet](https://parquet.apache.org/)columnar storage format, native to Spark, is ideal for this. RasterFrames
  //  work just like any other DataFrame in this scenario as long as @scaladoc[`rfInit`][rfInit] is called to register
  //  the imagery types.
  //
  //
  //    Let's assume we have a RasterFrameLayer we've done some fancy processing on:
  val equalizer = udf((t: Tile) => t.equalize())
  val equalized = rf.withColumn("equalized", equalizer($"tile")).asLayer

  equalized.printSchema
  equalized.select(rf_agg_stats($"tile")).show(false)
  equalized.select(rf_agg_stats($"equalized")).show(false)


  //  We write it out just like any other DataFrame, including the ability to specify partitioning:


  val filePath = "/tmp/equalized.parquet"
  equalized.select("*", "spatial_key.*").write.partitionBy("col", "row").mode(SaveMode.Overwrite).parquet(filePath)

  //  Let's confirm partitioning happened as expected:

  import java.io.File
  new File(filePath).list.filter(f => !f.contains("_"))

  //  Now we can load the data back in and check it out:

  val rf2 = spark.read.parquet(filePath)

  rf2.printSchema
  equalized.select(rf_agg_stats($"tile")).show(false)
  equalized.select(rf_agg_stats($"equalized")).show(false)

  //  ## Converting to `RDD` and `TileLayerRDD`
  //
  //  Since a `RasterFrameLayer` is just a `DataFrame` with extra metadata, the method
  //  @scaladoc[`DataFrame.rdd`][rdd] is available for simple conversion back to `RDD` space. The type returned
  //  by `.rdd` is dependent upon how you select it.


  import scala.reflect.runtime.universe._
  def showType[T: TypeTag](t: T) = println(implicitly[TypeTag[T]].tpe.toString)

  showType(rf.rdd)

  showType(rf.select(rf.spatialKeyColumn, $"tile".as[Tile]).rdd)

  showType(rf.select(rf.spatialKeyColumn, $"tile").as[(SpatialKey, Tile)].rdd)

  //  If your goal convert a single tile column with its spatial key back to a `TileLayerRDD[K]`, then there's an additional
  //  extension method on `RasterFrameLayer` called [`toTileLayerRDD`][toTileLayerRDD], which preserves the tile layer metadata,
  //  enhancing interoperation with GeoTrellis RDD extension methods.

  showType(rf.toTileLayerRDD($"tile".as[Tile]))

  //  ## Exporting a Raster
  //
  //  For the purposes of debugging, the RasterFrameLayer tiles can be reassembled back into a raster for viewing. However,
  //  keep in mind that this will download all the data to the driver, and reassemble it in-memory. So it's not appropriate
  //  for very large coverages.
  //
  //    Here's how one might render a raster frame to a false color PNG file:
  //


  val image = equalized.toRaster($"equalized", 774, 500)
  val colors = ColorMap.fromQuantileBreaks(image.tile.histogram, ColorRamps.BlueToOrange)
  image.tile.color(colors).renderPng().write("target/scala-2.11/tut/rf-raster.png")

  //  ![](rf-raster.png)

  //  Here's how one might render the image to a georeferenced GeoTIFF file:

  import geotrellis.raster.io.geotiff.GeoTiff
  GeoTiff(image).write("target/scala-2.11/tut/rf-raster.tiff")

  //  [*Download GeoTIFF*](rf-raster.tiff)

  //  # Exporting to a GeoTrellis Layer
  // First, convert the RasterFrameLayer into a TileLayerRDD. The return type is an Either;
  // the `left` side is for spatial-only keyed data
  val tlRDD = equalized.toTileLayerRDD($"equalized").left.get

  // First create a GeoTrellis layer writer
  val p = Files.createTempDirectory("gt-store")
  val writer: LayerWriter[LayerId] = LayerWriter(p.toUri)

  val layerId = LayerId("equalized", 0)
  writer.write(layerId, tlRDD, ZCurveKeyIndexMethod)

  spark.stop()
}
