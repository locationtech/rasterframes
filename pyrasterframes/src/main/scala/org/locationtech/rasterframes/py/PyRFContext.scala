/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2019 Astraea, Inc.
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
package org.locationtech.rasterframes.py

import java.nio.ByteBuffer

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, MultibandTile}
import geotrellis.spark._
import geotrellis.layer._
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.locationtech.rasterframes
import org.locationtech.rasterframes.extensions.RasterJoin
import org.locationtech.rasterframes.model.LazyCRS
import org.locationtech.rasterframes.ref.{GDALRasterSource, RasterRef, RFRasterSource}
import org.locationtech.rasterframes.util.KryoSupport
import org.locationtech.rasterframes.{RasterFunctions, _}
import spray.json._
import org.locationtech.rasterframes.util.JsonCodecs._
import scala.collection.JavaConverters._

/**
 * py4j access wrapper to RasterFrameLayer entry points.
 *
 * @since 11/6/17
 */
class PyRFContext(implicit sparkSession: SparkSession) extends RasterFunctions
  with org.locationtech.geomesa.spark.jts.DataFrameFunctions.Library {

  sparkSession.withRasterFrames

  def buildInfo(): java.util.HashMap[String, String] = {
    val retval = new java.util.HashMap[String, String]()
    RFBuildInfo.toMap.foreach {
      case (k, v) => retval.put(k, String.valueOf(v))
    }
    retval.put("GDAL", GDALRasterSource.gdalVersion())
    retval
  }

  def toSpatialMultibandTileLayerRDD(rf: RasterFrameLayer): MultibandTileLayerRDD[SpatialKey] =
    rf.toMultibandTileLayerRDD match {
      case Left(spatial) => spatial
      case Right(other) => throw new Exception(s"Expected a MultibandTileLayerRDD[SpatailKey] but got $other instead")
    }

  def toSpaceTimeMultibandTileLayerRDD(rf: RasterFrameLayer): MultibandTileLayerRDD[SpaceTimeKey] =
    rf.toMultibandTileLayerRDD match {
      case Right(temporal) => temporal
      case Left(other) => throw new Exception(s"Expected a MultibandTileLayerRDD[SpaceTimeKey] but got $other instead")
    }

  /**
   * Converts a `ContextRDD[Spatialkey, MultibandTile, TileLayerMedadata[Spatialkey]]` to a RasterFrameLayer
   */
  def asLayer(
    layer: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    bandCount: java.lang.Integer
  ): RasterFrameLayer = {
    implicit val pr = PairRDDConverter.forSpatialMultiband(bandCount.toInt)
    layer.toLayer
  }

  /**
   * Converts a `ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMedadata[SpaceTimeKey]]` to a RasterFrameLayer
   */
  def asLayer(
    layer: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]],
    bandCount: java.lang.Integer
  )(implicit d: DummyImplicit): RasterFrameLayer = {
    implicit val pr = PairRDDConverter.forSpaceTimeMultiband(bandCount.toInt)
    layer.toLayer
  }

  /**
    * Base conversion to RasterFrameLayer
    */
  def asLayer(df: DataFrame): RasterFrameLayer = {
    df.asLayer
  }

  /**
    * Conversion to RasterFrameLayer with spatial key column and TileLayerMetadata specified.
    */
  def asLayer(df: DataFrame, spatialKey: Column, tlm: String): RasterFrameLayer = {
    val jtlm = tlm.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    df.asLayer(spatialKey, jtlm)
  }

  /**
    * Left spatial join managing reprojection and merging of `other`
    */
  def rasterJoin(df: DataFrame, other: DataFrame): DataFrame = RasterJoin(df, other, None)

  /**
    * Left spatial join managing reprojection and merging of `other`; uses extent and CRS columns to determine if rows intersect
    */
  def rasterJoin(df: DataFrame, other: DataFrame, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column): DataFrame =
    RasterJoin(df, other, leftExtent, leftCRS, rightExtent, rightCRS, None)

  /**
    * Left spatial join managing reprojection and merging of `other`; uses joinExprs to conduct initial join then extent and CRS columns to determine if rows intersect
    */
  def rasterJoin(df: DataFrame, other: DataFrame, joinExprs: Column, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column): DataFrame =
    RasterJoin(df, other, joinExprs, leftExtent, leftCRS, rightExtent, rightCRS, None)


  /**
    * Convenience functions for use in Python
    */
  def _parse_cell_type(name: String): CellType = CellType.fromName(name)

  /**
    * Convenience list of valid cell type strings
    * @return Java List of String, which py4j can interpret as a python `list`
    */
  def rf_cell_types = {
    org.locationtech.rasterframes.functions.cellTypes().asJava
  }

  def rf_explode_tiles_sample(sampleFraction: Double, seed: Long, cols: Column*): Column =
    rf_explode_tiles_sample(sampleFraction, Some(seed), cols: _*)

  def tileColumns(df: DataFrame): Array[Column] =
    df.asLayer.tileColumns.toArray

  def spatialKeyColumn(df: DataFrame): Column =
    df.asLayer.spatialKeyColumn

  def temporalKeyColumn(df: DataFrame): Column =
    df.asLayer.temporalKeyColumn.orNull

  // All the scalar tile arithmetic functions

  def rf_local_add_double(col: Column, scalar: Double): Column = rf_local_add[Double](col, scalar)

  def rf_local_add_int(col: Column, scalar: Int): Column = rf_local_add[Int](col, scalar)

  def rf_local_subtract_double(col: Column, scalar: Double): Column = rf_local_subtract[Double](col, scalar)

  def rf_local_subtract_int(col: Column, scalar: Int): Column = rf_local_subtract[Int](col, scalar)

  def rf_local_divide_double(col: Column, scalar: Double): Column = rf_local_divide[Double](col, scalar)

  def rf_local_divide_int(col: Column, scalar: Int): Column = rf_local_divide[Int](col, scalar)

  def rf_local_multiply_double(col: Column, scalar: Double): Column = rf_local_multiply[Double](col, scalar)

  def rf_local_multiply_int(col: Column, scalar: Int): Column = rf_local_multiply[Int](col, scalar)

  def rf_local_less_double(col: Column, scalar: Double): Column = rf_local_less[Double](col, scalar)

  def rf_local_less_int(col: Column, scalar: Int): Column = rf_local_less[Int](col, scalar)

  def rf_local_less_equal_double(col: Column, scalar: Double): Column = rf_local_less_equal[Double](col, scalar)

  def rf_local_less_equal_int(col: Column, scalar: Int): Column = rf_local_less_equal[Int](col, scalar)

  def rf_local_greater_double(col: Column, scalar: Double): Column = rf_local_greater[Double](col, scalar)

  def rf_local_greater_int(col: Column, scalar: Int): Column = rf_local_greater[Int](col, scalar)

  def rf_local_greater_equal_double(col: Column, scalar: Double): Column = rf_local_greater_equal[Double](col, scalar)

  def rf_local_greater_equal_int(col: Column, scalar: Int): Column = rf_local_greater_equal[Int](col, scalar)

  def rf_local_equal_double(col: Column, scalar: Double): Column = rf_local_equal[Double](col, scalar)

  def rf_local_equal_int(col: Column, scalar: Int): Column = rf_local_equal[Int](col, scalar)

  def rf_local_unequal_double(col: Column, scalar: Double): Column = rf_local_unequal[Double](col, scalar)

  def rf_local_unequal_int(col: Column, scalar: Int): Column = rf_local_unequal[Int](col, scalar)

  // other function support
  /** py4j friendly version of this function */
  def rf_agg_approx_quantiles(tile: Column, probabilities: java.util.List[Double], relativeError: Double): TypedColumn[Any, Seq[Double]] = {
    import scala.collection.JavaConverters._
    rf_agg_approx_quantiles(tile, probabilities.asScala, relativeError)
  }

  def _make_crs_literal(crsText: String): Column = {
    rasterframes.encoders.serialized_literal[CRS](LazyCRS(crsText))
  }

  // return toRaster, get just the tile, and make an array out of it
  def toIntRaster(df: DataFrame, colname: String, cols: Int, rows: Int): Array[Int] = {
    df.asLayer.toRaster(df.col(colname), cols, rows).tile.toArray()
  }

  def toDoubleRaster(df: DataFrame, colname: String, cols: Int, rows: Int): Array[Double] = {
    df.asLayer.toRaster(df.col(colname), cols, rows).tile.toArrayDouble()
  }

  def tileLayerMetadata(df: DataFrame): String =
    // The `fold` is required because an `Either` is retured, depending on the key type.
    df.asLayer.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint

  def spatialJoin(df: DataFrame, right: DataFrame): RasterFrameLayer = df.asLayer.spatialJoin(right.asLayer)

  def withBounds(df: DataFrame): RasterFrameLayer = df.asLayer.withGeometry()

  def withCenter(df: DataFrame): RasterFrameLayer = df.asLayer.withCenter()

  def withCenterLatLng(df: DataFrame): RasterFrameLayer = df.asLayer.withCenterLatLng()

  def withSpatialIndex(df: DataFrame): RasterFrameLayer = df.asLayer.withSpatialIndex()

  //----------------------------Support Routines-----------------------------------------

  def _listToSeq(cols: java.util.ArrayList[AnyRef]): Seq[AnyRef] = cols.asScala

  type jInt = java.lang.Integer
  type jDouble = java.lang.Double
  // NB: Tightly coupled to the `RFContext.resolve_raster_ref` method in `pyrasterframes.rf_context`. */
  def _resolveRasterRef(srcBin: Array[Byte], bandIndex: jInt, xmin: jDouble, ymin: jDouble, xmax: jDouble, ymax: jDouble): AnyRef = {
    val src = KryoSupport.deserialize[RFRasterSource](ByteBuffer.wrap(srcBin))
    val extent = Extent(xmin, ymin, xmax, ymax)
    RasterRef(src, bandIndex, Some(extent), None)
  }

  def _dfToMarkdown(df: DataFrame, numRows: Int, truncate: Boolean): String = {
    import rasterframes.util.DFWithPrettyPrint
    df.toMarkdown(numRows, truncate, renderTiles = true)
  }

  def _dfToHTML(df: DataFrame, numRows: Int, truncate: Boolean): String = {
    import rasterframes.util.DFWithPrettyPrint
    df.toHTML(numRows, truncate, renderTiles = true)
  }

  def _reprojectExtent(extent: Extent, srcCRS: String, destCRS: String): Extent =
    extent.reproject(LazyCRS(srcCRS), LazyCRS(destCRS))
}
