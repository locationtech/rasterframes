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

/**
 *
 * @author sfitch 
 * @since 11/6/17
 */
object CreatingRasterFrames extends App {
//  # Creating RasterFrames
//
//  There are a number of ways to create a `RasterFrame`, as enumerated in the sections below.
//
//  ## Initialization
//
//  First, some standard `import`s:

  import org.locationtech.rasterframes._
  import geotrellis.raster._
  import geotrellis.raster.io.geotiff.SinglebandGeoTiff
  import geotrellis.spark.io._
  import org.apache.spark.sql._

//  Next, initialize the `SparkSession`, and call the `withRasterFrames` method on it:

  implicit val spark = SparkSession.builder().
    master("local[*]").appName("RasterFrames").
    getOrCreate().
    withRasterFrames
  spark.sparkContext.setLogLevel("ERROR")

//  ## From `ProjectedExtent`
//
//  The simplest mechanism for getting a RasterFrame is to use the `toRF(tileCols, tileRows)` extension method on `ProjectedRaster`.

  val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
  val rf = scene.projectedRaster.toRF(128, 128)
  rf.show(5, false)


//  ## From `TileLayerRDD`
//
//  Another option is to use a GeoTrellis [`LayerReader`](https://docs.geotrellis.io/en/latest/guide/tile-backends.html), to get a `TileLayerRDD` for which there's also a `toRF` extension method.


//  ## Inspecting Structure
//
//  `RasterFrame` has a number of methods providing access to metadata about the contents of the RasterFrame.
//
//  ### Tile Column Names

  rf.tileColumns.map(_.toString)

//  ### Spatial Key Column Name

  rf.spatialKeyColumn.toString

//  ### Temporal Key Column
//
//  Returns an `Option[Column]` since not all RasterFrames have an explicit temporal dimension.

  rf.temporalKeyColumn.map(_.toString)

//  ### Tile Layer Metadata
//
//  The Tile Layer Metadata defines how the spatial/spatiotemporal domain is discretized into tiles,
//  and what the key bounds are.

  import spray.json._
  // The `fold` is required because an `Either` is retured, depending on the key type.
  rf.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint

  spark.stop()
}
