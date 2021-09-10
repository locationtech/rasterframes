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
 * SPDX-License-Identifier: Apache-2.0
 *
 */
package org.locationtech.rasterframes
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.{Tile, _}
import geotrellis.vector.Extent
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SQLContext}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.util.ResampleMethod

/**
 * Module utils.
 *
 * @since 9/7/17
 */
package object functions {

  @inline
  private[rasterframes] def safeBinaryOp[T <: AnyRef, R >: T](op: (T, T) => R): (T, T) => R =
    (o1: T, o2: T) => {
      if (o1 == null) o2
      else if (o2 == null) o1
      else op(o1, o2)
    }
  @inline
  private[rasterframes] def safeEval[P, R <: AnyRef](f: P => R): P => R =
    p => if (p == null) null.asInstanceOf[R] else f(p)
  @inline
  private[rasterframes] def safeEval[P](f: P => Double)(implicit d: DummyImplicit): P => Double =
    p => if (p == null) Double.NaN else f(p)
  @inline
  private[rasterframes] def safeEval[P](f: P => Long)(implicit d1: DummyImplicit, d2: DummyImplicit): P => Long =
    p => if (p == null) 0l else f(p)
  @inline
  private[rasterframes] def safeEval[P1, P2, R](f: (P1, P2) => R): (P1, P2) => R =
    (p1, p2) => if (p1 == null || p2 == null) null.asInstanceOf[R] else f(p1, p2)

  /** Converts an array into a tile. */
  private[rasterframes] def arrayToTile(cols: Int, rows: Int) = {
    safeEval[AnyRef, Tile]{
      case s: Seq[_] => s.headOption match {
        case Some(_: Int) => RawArrayTile(s.asInstanceOf[Seq[Int]].toArray[Int], cols, rows)
        case Some(_: Double) => RawArrayTile(s.asInstanceOf[Seq[Double]].toArray[Double], cols, rows)
        case Some(_: Byte) => RawArrayTile(s.asInstanceOf[Seq[Byte]].toArray[Byte], cols, rows)
        case Some(_: Short) => RawArrayTile(s.asInstanceOf[Seq[Short]].toArray[Short], cols, rows)
        case Some(_: Float) => RawArrayTile(s.asInstanceOf[Seq[Float]].toArray[Float], cols, rows)
        case Some(o @ _) => throw new MatchError(o)
        case None => null
      }
    }
  }

  private[rasterframes] val arrayToTileFunc3: (Array[Double], Int, Int) => Tile = (a, cols, rows) => {
    arrayToTile(cols, rows).apply(a)
  }

  /** Constructor for constant tiles */
  private[rasterframes] val makeConstantTile: (Number, Int, Int, String) => Tile = (value, cols, rows, cellTypeName) => {
    val cellType = CellType.fromName(cellTypeName)
    cellType match {
      case BitCellType => BitConstantTile(if (value.intValue() == 0) false else true, cols, rows)
      case ct: ByteCells => ByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: UByteCells => UByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: ShortCells => ShortConstantTile(value.shortValue(), cols, rows, ct)
      case ct: UShortCells => UShortConstantTile(value.shortValue(), cols, rows, ct)
      case ct: IntCells => IntConstantTile(value.intValue(), cols, rows, ct)
      case ct: FloatCells => FloatConstantTile(value.floatValue(), cols, rows, ct)
      case ct: DoubleCells => DoubleConstantTile(value.doubleValue(), cols, rows, ct)
    }
  }

  /** Alias for constant tiles of zero */
  private[rasterframes] val tileZeros: (Int, Int, String) => Tile = (cols, rows, cellTypeName) =>
    makeConstantTile(0, cols, rows, cellTypeName)

  /** Alias for constant tiles of one */
  private[rasterframes] val tileOnes: (Int, Int, String) => Tile = (cols, rows, cellTypeName) =>
    makeConstantTile(1, cols, rows, cellTypeName)

  val reproject_and_merge_f: (Row, CRS, Seq[Tile], Seq[Row], Seq[CRS], Row, String) => Tile = (leftExtentEnc: Row, leftCRSEnc: CRS, tiles: Seq[Tile], rightExtentEnc: Seq[Row], rightCRSEnc: Seq[CRS], leftDimsEnc: Row, resampleMethod: String) => {
    if (tiles.isEmpty) null
    else {
      require(tiles.length == rightExtentEnc.length && tiles.length == rightCRSEnc.length, "size mismatch")

      // https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-RowEncoder.html
      // import org.apache.spark.sql.catalyst.encoders.RowEncoder
      // WOW TODO: Row Encoder all over the places
      // println(
        /*extentEncoder
          .resolveAndBind() // bind it to schema before deserializing, that's how spark Dataset.as works
                            // see https://github.com/apache/spark/blob/93cec49212fe82816fcadf69f429cebaec60e058/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala#L75-L86
          .createDeserializer()(
            RowEncoder(extentEncoder.schema)
              .createSerializer()(leftExtentEnc)
          )*/
      // )
      val leftExtent: Extent = leftExtentEnc match {
        case Row(xmin: Double, ymin: Double, xmax: Double, ymax: Double) => Extent(xmin, ymin, xmax, ymax)
      }
      val leftDims: Dimensions[Int] = leftDimsEnc match {
        case Row(cols: Int, rows: Int) => Dimensions(cols, rows)
      }
      val leftCRS: CRS = leftCRSEnc
      lazy val rightExtents: Seq[Extent] = rightExtentEnc.map {
        case Row(xmin: Double, ymin: Double, xmax: Double, ymax: Double) => Extent(xmin, ymin, xmax, ymax)
      }
      lazy val rightCRSs: Seq[CRS] = rightCRSEnc
      lazy val resample = resampleMethod match {
        case ResampleMethod(mm) => mm
        case _ => throw new IllegalArgumentException(s"Unable to parse ResampleMethod for ${resampleMethod}.")
      }

      if (leftExtent == null || leftDims == null || leftCRS == null) null
      else {

        val cellType = tiles.map(_.cellType).reduceOption(_ union _).getOrElse(tiles.head.cellType)

        // TODO: how to allow control over... expression?
        val projOpts = Reproject.Options(resample)
        val dest: Tile = ArrayTile.empty(cellType, leftDims.cols, leftDims.rows)
        //is there a GT function to do all this?
        tiles.zip(rightExtents).zip(rightCRSs).map {
          case ((tile, extent), crs) =>
            tile.reproject(extent, crs, leftCRS, projOpts)
        }.foldLeft(dest)((d, t) =>
          d.merge(leftExtent, t.extent, t.tile, projOpts.method)
        )
      }
    }
  }

  // NB: Don't be tempted to make this a `val`. Spark will barf if `withRasterFrames` hasn't been called first.
  def reproject_and_merge = udf(reproject_and_merge_f)
    .withName("reproject_and_merge")


  private[rasterframes] val cellTypes: () => Seq[String] = () =>
    Seq(
      BitCellType,
      ByteCellType,
      ByteConstantNoDataCellType,
      UByteCellType,
      UByteConstantNoDataCellType,
      ShortCellType,
      ShortConstantNoDataCellType,
      UShortCellType,
      UShortConstantNoDataCellType,
      IntCellType,
      IntConstantNoDataCellType,
      FloatCellType,
      FloatConstantNoDataCellType,
      DoubleCellType,
      DoubleConstantNoDataCellType
    ).map(_.toString).distinct

  /**
   * Rasterize geometry into tiles.
   */
  private[rasterframes] val rasterize: (Geometry, Geometry, Int, Int, Int) => Tile = {
    (geom, bounds, value, cols, rows) => {
      // We have to do this because (as of spark 2.2.x) Encoder-only types
      // can't be used as UDF inputs. Only Spark-native types and UDTs.
      val extent = Extent(bounds.getEnvelopeInternal)
      geom.rasterizeWithValue(RasterExtent(extent, cols, rows), value).tile
    }
  }

  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rf_make_constant_tile", makeConstantTile)
    sqlContext.udf.register("rf_make_zeros_tile", tileZeros)
    sqlContext.udf.register("rf_make_ones_tile", tileOnes)
    sqlContext.udf.register("rf_cell_types", cellTypes)
    sqlContext.udf.register("rf_rasterize", rasterize)
    // sqlContext.udf.register("rf_array_to_tile", arrayToTileFunc3)
  }
}
