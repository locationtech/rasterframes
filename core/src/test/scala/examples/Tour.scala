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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import org.locationtech.rasterframes._
import geotrellis.raster.render.{ColorRamps, IndexedColorMap}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.locationtech.rasterframes.ml.TileExploder

/**
 * Example tour of some general features in RasterFrames
 *
 * @since 10/24/17
 */
object Tour extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .getOrCreate()
    .withRasterFrames

  import spark.implicits._

  // Read in a geo-referenced image
  val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")

  // Convert it to a raster frame, discretizing it into the given tile size.
  val rf = scene.projectedRaster.toRF(64, 64)

  // See how many tiles we have after discretization
  println("Tile count: " + rf.count())

  // Take a peek at what we're working with
  rf.show(8, false)

  // Confirm we have equally sized tiles
  rf.select(rf_dimensions($"tile")).distinct().show()

  // Count the number of no-data cells
  rf.select(rf_agg_no_data_cells($"tile")).show(false)

  // Compute per-tile statistics
  rf.select(rf_tile_stats($"tile")).show(8, false)

  // Compute some aggregate stats over all cells
  rf.select(rf_agg_stats($"tile")).show(false)

  // Create a Spark UDT to perform contrast adjustment via GeoTrellis
  val contrast = udf((t: Tile) ⇒ t.sigmoidal(0.2, 10))

  // Let's contrast adjust the tile column
  val withAdjusted = rf.withColumn("adjusted", contrast($"tile")).asRF

  // Show the stats for the adjusted version
  withAdjusted.select(rf_agg_stats($"adjusted")).show(false)

  // Reassemble into a raster and save to a file
  val raster = withAdjusted.toRaster($"adjusted", 774, 500)
  GeoTiff(raster).write("contrast-adjusted.tiff")

  // Perform some arbitrary local ops between columns and render
  val withOp = withAdjusted.withColumn("op", rf_local_subtract($"tile", $"adjusted")).asRF
  val raster2 = withOp.toRaster($"op", 774, 500)
  GeoTiff(raster2).write("with-op.tiff")


  // Perform k-means clustering
  val k = 4

  // SparkML doesn't like NoData/NaN values, so we set the no-data value to something less offensive
  val forML = rf.select(rf.spatialKeyColumn, rf_with_no_data($"tile", 99999) as "tile").asRF

  // First we instantiate the transformer that converts tile rows into cell rows.
  val exploder = new TileExploder()

  // This transformer wraps the pixel values in a vector.
  // Could use this with multiple bands
  val assembler = new VectorAssembler().
    setInputCols(Array("tile")).
    setOutputCol("features")

  // Or clustering algorithm
  val kmeans = new KMeans().setK(k)

  // Construct the ML pipeline
  val pipeline = new Pipeline().setStages(Array(exploder, assembler, kmeans))

  // Compute the model
  val model = pipeline.fit(forML)

  // Score the data
  val clusteredCells = model.transform(forML)

  clusteredCells.show()

  clusteredCells.groupBy("prediction").count().show

  // Reassembling the clustering results takes a number of steps.
  val tlm = rf.tileLayerMetadata.left.get

  // RasterFrames provides a special aggregation function for assembling tiles from cells with column/row indexes
  val retiled = clusteredCells.groupBy(forML.spatialKeyColumn).agg(
    rf_assemble_tile($"column_index", $"row_index", $"prediction", tlm.tileCols, tlm.tileRows, ByteConstantNoDataCellType)
  )

  val clusteredRF = retiled.asRF($"spatial_key", tlm)

  val raster3 = clusteredRF.toRaster($"prediction", 774, 500)

  val clusterColors = IndexedColorMap.fromColorMap(
    ColorRamps.Viridis.toColorMap((0 until k).toArray)
  )

  GeoTiff(raster3).copy(options = GeoTiffOptions(clusterColors)).write("clustered.tiff")

  spark.stop()
}
