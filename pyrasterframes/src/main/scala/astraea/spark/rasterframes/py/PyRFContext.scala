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
   * Converts a ContextRDD[Spatialkey, MultibandTile, TileLayerMedadata[Spatialkey]] to a RasterFrame
   */
  def asRF(
    layer: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    bandCount: java.lang.Integer
  ): RasterFrame = {
    implicit val pr = PairRDDConverter.forSpatialMultiband(bandCount.toInt)
    layer.toRF
  }

  /**
   * Converts a ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMedadata[SpaceTimeKey]] to a RasterFrame
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
  def cellType(name: String): CellType = CellType.fromName(name)

  def cellTypes: Seq[String] = astraea.spark.rasterframes.functions.cellTypes()

  /** DESERIALIZATION **/

  def generateTile(cellType: String, cols: Int, rows: Int, bytes: Array[Byte]): ArrayTile = {
    ArrayTile.fromBytes(bytes, this.cellType(cellType), cols, rows)
  }

  def generateGeometry(obj: Array[Byte]): Geometry =  WKBUtils.read(obj)

  def explodeTilesSample(sampleFraction: Double, seed: Long, cols: Column*): Column =
    explodeTilesSample(sampleFraction, Some(seed), cols: _*)


  def tileColumns(df: DataFrame): Array[Column] =
    df.asRF.tileColumns.toArray

  def spatialKeyColumn(df: DataFrame): Column =
    df.asRF.spatialKeyColumn

  def temporalKeyColumn(df: DataFrame): Column =
    df.asRF.temporalKeyColumn.orNull

  def tileToIntArray(col: Column): Column = tileToArray[Int](col)

  def tileToDoubleArray(col: Column): Column = tileToArray[Double](col)

  // All the scalar tile arithmetic functions

  def localAddScalar(col: Column, scalar: Double): Column = localAddScalar[Double](col, scalar)

  def localAddScalarInt(col: Column, scalar: Int): Column = localAddScalar[Int](col, scalar)

  def localSubtractScalar(col: Column, scalar: Double): Column = localSubtractScalar[Double](col, scalar)

  def localSubtractScalarInt(col: Column, scalar: Int): Column = localSubtractScalar[Int](col, scalar)

  def localDivideScalar(col: Column, scalar: Double): Column = localDivideScalar[Double](col, scalar)

  def localDivideScalarInt(col: Column, scalar: Int): Column = localDivideScalar[Int](col, scalar)

  def localMultiplyScalar(col: Column, scalar: Double): Column = localMultiplyScalar[Double](col, scalar)

  def localMultiplyScalarInt(col: Column, scalar: Int): Column = localMultiplyScalar[Int](col, scalar)

  def localLessScalar(col: Column, scalar: Double): Column = localLessScalar[Double](col, scalar)

  def localLessScalarInt(col: Column, scalar: Int): Column = localLessScalar[Int](col, scalar)

  def localLessEqualScalar(col: Column, scalar: Double): Column = localLessEqualScalar[Double](col, scalar)

  def localLessEqualScalarInt(col: Column, scalar: Int): Column = localLessEqualScalar[Int](col, scalar)

  def localGreaterScalar(col: Column, scalar: Double): Column = localGreaterScalar[Double](col, scalar)

  def localGreaterScalarInt(col: Column, scalar: Int): Column = localGreaterScalar[Int](col, scalar)

  def localGreaterEqualScalar(col: Column, scalar: Double): Column = localGreaterEqualScalar[Double](col, scalar)

  def localGreaterEqualScalarInt(col: Column, scalar: Int): Column = localGreaterEqualScalar[Int](col, scalar)

  def localEqualScalar(col: Column, scalar: Double): Column = localEqualScalar[Double](col, scalar)

  def localEqualScalarInt(col: Column, scalar: Int): Column = localEqualScalar[Int](col, scalar)

  def localUnequalScalar(col: Column, scalar: Double): Column = localUnequalScalar[Double](col, scalar)

  def localUnequalScalarInt(col: Column, scalar: Int): Column = localUnequalScalar[Int](col, scalar)

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

  def reprojectGeometry(geometryCol: Column, srcName: String, dstName: String): Column = {
    val src = CRSParser(srcName)
    val dst = CRSParser(dstName)
    reprojectGeometry(geometryCol, src, dst)
  }

  def listToSeq(cols: java.util.ArrayList[AnyRef]): Seq[AnyRef] = cols.asScala
}
