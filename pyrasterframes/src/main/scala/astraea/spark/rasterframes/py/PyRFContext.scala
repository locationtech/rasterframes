/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2018 Astraea, Inc.
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
package astraea.spark.rasterframes.py

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util.CRSParser
import com.vividsolutions.jts.geom.Geometry
import geotrellis.raster.{ArrayTile, CellType, MultibandTile}
import geotrellis.spark.io._
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import org.apache.spark.sql._
import org.locationtech.geomesa.spark.jts.util.WKBUtils
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
  def cell_type(name: String): CellType = CellType.fromName(name)

  /**
    * Convenience list of valid cell type strings
    * @return Java List of String, which py4j can interpret as a python `list`
    */
  def cell_types = {
    astraea.spark.rasterframes.functions.cellTypes().asJava
  }

  /** DESERIALIZATION **/

  def generate_tile(cellType: String, cols: Int, rows: Int, bytes: Array[Byte]): ArrayTile = {
    ArrayTile.fromBytes(bytes, this.cell_type(cellType), cols, rows)
  }

  def generate_geometry(obj: Array[Byte]): Geometry =  WKBUtils.read(obj)

  def explode_tiles_sample(sampleFraction: Double, seed: Long, cols: Column*): Column =
    explode_tiles_sample(sampleFraction, Some(seed), cols: _*)


  def tileColumns(df: DataFrame): Array[Column] =
    df.asRF.tileColumns.toArray

  def spatialKeyColumn(df: DataFrame): Column =
    df.asRF.spatialKeyColumn

  def temporalKeyColumn(df: DataFrame): Column =
    df.asRF.temporalKeyColumn.orNull

  def tile_to_int_array(col: Column): Column = tile_to_array[Int](col)

  def tile_to_double_array(col: Column): Column = tile_to_array[Double](col)

  // All the scalar tile arithmetic functions

  def local_add_scalar(col: Column, scalar: Double): Column = local_add_scalar[Double](col, scalar)

  def local_add_scalar_int(col: Column, scalar: Int): Column = local_add_scalar[Int](col, scalar)

  def local_subtract_scalar(col: Column, scalar: Double): Column = local_subtract_scalar[Double](col, scalar)

  def local_subtract_scalar_int(col: Column, scalar: Int): Column = local_subtract_scalar[Int](col, scalar)

  def local_divide_scalar(col: Column, scalar: Double): Column = local_divide_scalar[Double](col, scalar)

  def local_divide_scalar_int(col: Column, scalar: Int): Column = local_divide_scalar[Int](col, scalar)

  def local_multiply_scalar(col: Column, scalar: Double): Column = local_multiply_scalar[Double](col, scalar)

  def local_multiply_scalar_int(col: Column, scalar: Int): Column = local_multiply_scalar[Int](col, scalar)

  def local_less_scalar(col: Column, scalar: Double): Column = local_less_scalar[Double](col, scalar)

  def local_less_scalar_int(col: Column, scalar: Int): Column = local_less_scalar[Int](col, scalar)

  def local_less_equal_scalar(col: Column, scalar: Double): Column = local_less_equal_scalar[Double](col, scalar)

  def local_less_equal_scalar_int(col: Column, scalar: Int): Column = local_less_equal_scalar[Int](col, scalar)

  def local_greater_scalar(col: Column, scalar: Double): Column = local_greater_scalar[Double](col, scalar)

  def local_greater_scalar_int(col: Column, scalar: Int): Column = local_greater_scalar[Int](col, scalar)

  def local_greater_equal_scalar(col: Column, scalar: Double): Column = local_greater_equal_scalar[Double](col, scalar)

  def local_greater_equal_scalar_int(col: Column, scalar: Int): Column = local_greater_equal_scalar[Int](col, scalar)

  def local_equal_scalar(col: Column, scalar: Double): Column = local_equal_scalar[Double](col, scalar)

  def local_equal_scalar_int(col: Column, scalar: Int): Column = local_equal_scalar[Int](col, scalar)

  def local_unequal_scalar(col: Column, scalar: Double): Column = local_unequal_scalar[Double](col, scalar)

  def local_unequal_scalar_int(col: Column, scalar: Int): Column = local_unequal_scalar[Int](col, scalar)

  // return toRaster, get just the tile, and make an array out of it
  def toIntRaster(df: DataFrame, colname: String, cols: Int, rows: Int): Array[Int] = {
    df.asRF.toRaster(df.col(colname), cols, rows).toArray()
  }

  def toDoubleRaster(df: DataFrame, colname: String, cols: Int, rows: Int): Array[Double] = {
    df.asRF.toRaster(df.col(colname), cols, rows).toArrayDouble()
  }

  def tileLayerMetadata(df: DataFrame): String =
    // The `fold` is required because an `Either` is retured, depending on the key type.
    df.asRF.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint

  def spatialJoin(df: DataFrame, right: DataFrame): RasterFrame = df.asRF.spatialJoin(right.asRF)

  def withBounds(df: DataFrame): RasterFrame = df.asRF.withBounds()

  def withCenter(df: DataFrame): RasterFrame = df.asRF.withCenter()

  def withCenterLatLng(df: DataFrame): RasterFrame = df.asRF.withCenterLatLng()

  def withSpatialIndex(df: DataFrame): RasterFrame = df.asRF.withSpatialIndex()

  def reproject_geometry(geometryCol: Column, srcName: String, dstName: String): Column = {
    val src = CRSParser(srcName)
    val dst = CRSParser(dstName)
    reproject_geometry(geometryCol, src, dst)
  }

  def listToSeq(cols: java.util.ArrayList[AnyRef]): Seq[AnyRef] = cols.asScala
}
