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

import geotrellis.raster.{CellType, MultibandTile}
import geotrellis.spark.io._
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import org.apache.spark.sql._
import org.locationtech.rasterframes.{RasterFunctions, _}
import org.locationtech.rasterframes.model.LazyCRS
import spray.json._

import scala.collection.JavaConverters._

/**
 * py4j access wrapper to RasterFrame entry points.
 *
 * @since 11/6/17
 */
class PyRFContext(implicit sparkSession: SparkSession) extends RasterFunctions
  with org.locationtech.geomesa.spark.jts.DataFrameFunctions.Library {

  sparkSession.withRasterFrames

  def toSpatialMultibandTileLayerRDD(rf: RasterFrame): MultibandTileLayerRDD[SpatialKey] =
    rf.toMultibandTileLayerRDD match {
      case Left(spatial) => spatial
      case Right(other) => throw new Exception(s"Expected a MultibandTileLayerRDD[SpatailKey] but got $other instead")
    }

  def toSpaceTimeMultibandTileLayerRDD(rf: RasterFrame): MultibandTileLayerRDD[SpaceTimeKey] =
    rf.toMultibandTileLayerRDD match {
      case Right(temporal) => temporal
      case Left(other) => throw new Exception(s"Expected a MultibandTileLayerRDD[SpaceTimeKey] but got $other instead")
    }

  /**
   * Converts a `ContextRDD[Spatialkey, MultibandTile, TileLayerMedadata[Spatialkey]]` to a RasterFrame
   */
  def asRF(
    layer: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    bandCount: java.lang.Integer
  ): RasterFrame = {
    implicit val pr = PairRDDConverter.forSpatialMultiband(bandCount.toInt)
    layer.toRF
  }

  /**
   * Converts a `ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMedadata[SpaceTimeKey]]` to a RasterFrame
   */
  def asRF(
    layer: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]],
    bandCount: java.lang.Integer
  )(implicit d: DummyImplicit): RasterFrame = {
    implicit val pr = PairRDDConverter.forSpaceTimeMultiband(bandCount.toInt)
    layer.toRF
  }

  /**
    * Base conversion to RasterFrame
    */
  def asRF(df: DataFrame): RasterFrame = {
    df.asRF
  }

  /**
    * Conversion to RasterFrame with spatial key column and TileLayerMetadata specified.
    */
  def asRF(df: DataFrame, spatialKey: Column, tlm: String): RasterFrame = {
    val jtlm = tlm.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    df.asRF(spatialKey, jtlm)
  }

  /**
    * Convenience functions for use in Python
    */
  def rf_cell_type(name: String): CellType = CellType.fromName(name)

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
    df.asRF.tileColumns.toArray

  def spatialKeyColumn(df: DataFrame): Column =
    df.asRF.spatialKeyColumn

  def temporalKeyColumn(df: DataFrame): Column =
    df.asRF.temporalKeyColumn.orNull

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

  def rf_reproject_geometry(geometryCol: Column, srcName: String, dstName: String): Column = {
    val src = LazyCRS(srcName)
    val dst = LazyCRS(dstName)
    rf_reproject_geometry(geometryCol, src, dst)
  }

  // return toRaster, get just the tile, and make an array out of it
  def toIntRaster(df: DataFrame, colname: String, cols: Int, rows: Int): Array[Int] = {
    df.asRF.toRaster(df.col(colname), cols, rows).tile.toArray()
  }

  def toDoubleRaster(df: DataFrame, colname: String, cols: Int, rows: Int): Array[Double] = {
    df.asRF.toRaster(df.col(colname), cols, rows).tile.toArrayDouble()
  }

  def tileLayerMetadata(df: DataFrame): String =
    // The `fold` is required because an `Either` is retured, depending on the key type.
    df.asRF.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint

  def spatialJoin(df: DataFrame, right: DataFrame): RasterFrame = df.asRF.spatialJoin(right.asRF)

  def withBounds(df: DataFrame): RasterFrame = df.asRF.withGeometry()

  def withCenter(df: DataFrame): RasterFrame = df.asRF.withCenter()

  def withCenterLatLng(df: DataFrame): RasterFrame = df.asRF.withCenterLatLng()

  def withSpatialIndex(df: DataFrame): RasterFrame = df.asRF.withSpatialIndex()

  //----------------------------Support Routines-----------------------------------------

  def _listToSeq(cols: java.util.ArrayList[AnyRef]): Seq[AnyRef] = cols.asScala
}
