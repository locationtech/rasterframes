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
import geotrellis.raster.{Tile, _}
import geotrellis.vector.Extent
import org.apache.spark.sql.SQLContext
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.jts.ReprojectionTransformer
import org.locationtech.rasterframes.model.LazyCRS

/**
 * Module utils.
 *
 * @since 9/7/17
 */
package object functions {

  @inline
  private[rasterframes] def safeBinaryOp[T <: AnyRef, R >: T](op: (T, T) ⇒ R): ((T, T) ⇒ R) =
    (o1: T, o2: T) ⇒ {
      if (o1 == null) o2
      else if (o2 == null) o1
      else op(o1, o2)
    }
  @inline
  private[rasterframes] def safeEval[P, R <: AnyRef](f: P ⇒ R): P ⇒ R =
    (p) ⇒ if (p == null) null.asInstanceOf[R] else f(p)
  @inline
  private[rasterframes] def safeEval[P](f: P ⇒ Double)(implicit d: DummyImplicit): P ⇒ Double =
    (p) ⇒ if (p == null) Double.NaN else f(p)
  @inline
  private[rasterframes] def safeEval[P](f: P ⇒ Long)(implicit d1: DummyImplicit, d2: DummyImplicit): P ⇒ Long =
    (p) ⇒ if (p == null) 0l else f(p)
  @inline
  private[rasterframes] def safeEval[P1, P2, R](f: (P1, P2) ⇒ R): (P1, P2) ⇒ R =
    (p1, p2) ⇒ if (p1 == null || p2 == null) null.asInstanceOf[R] else f(p1, p2)

  /** Converts an array into a tile. */
  private[rasterframes] def arrayToTile(cols: Int, rows: Int) = {
    safeEval[AnyRef, Tile]{
      case s: Seq[_] ⇒ s.headOption match {
        case Some(_: Int) ⇒ RawArrayTile(s.asInstanceOf[Seq[Int]].toArray[Int], cols, rows)
        case Some(_: Double) ⇒ RawArrayTile(s.asInstanceOf[Seq[Double]].toArray[Double], cols, rows)
        case Some(_: Byte) ⇒ RawArrayTile(s.asInstanceOf[Seq[Byte]].toArray[Byte], cols, rows)
        case Some(_: Short) ⇒ RawArrayTile(s.asInstanceOf[Seq[Short]].toArray[Short], cols, rows)
        case Some(_: Float) ⇒ RawArrayTile(s.asInstanceOf[Seq[Float]].toArray[Float], cols, rows)
        case Some(o @ _) ⇒ throw new MatchError(o)
        case None ⇒ null
      }
    }
  }

  /** Set the tile's no-data value. */
  private[rasterframes] def withNoData(nodata: Double) = safeEval[Tile, Tile](_.withNoData(Some(nodata)))

  /** Constructor for constant tiles */
  private[rasterframes] val makeConstantTile: (Number, Int, Int, String) ⇒ Tile = (value, cols, rows, cellTypeName) ⇒ {
    val cellType = CellType.fromName(cellTypeName)
    cellType match {
      case BitCellType ⇒ BitConstantTile(if (value.intValue() == 0) false else true, cols, rows)
      case ct: ByteCells ⇒ ByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: UByteCells ⇒ UByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: ShortCells ⇒ ShortConstantTile(value.shortValue(), cols, rows, ct)
      case ct: UShortCells ⇒ UShortConstantTile(value.shortValue(), cols, rows, ct)
      case ct: IntCells ⇒ IntConstantTile(value.intValue(), cols, rows, ct)
      case ct: FloatCells ⇒ FloatConstantTile(value.floatValue(), cols, rows, ct)
      case ct: DoubleCells ⇒ DoubleConstantTile(value.doubleValue(), cols, rows, ct)
    }
  }

  /** Alias for constant tiles of zero */
  private[rasterframes] val tileZeros: (Int, Int, String) ⇒ Tile = (cols, rows, cellTypeName) ⇒
    makeConstantTile(0, cols, rows, cellTypeName)

  /** Alias for constant tiles of one */
  private[rasterframes] val tileOnes: (Int, Int, String) ⇒ Tile = (cols, rows, cellTypeName) ⇒
    makeConstantTile(1, cols, rows, cellTypeName)


  private[rasterframes] val cellTypes: () ⇒ Seq[String] = () ⇒
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
  private[rasterframes] val rasterize: (Geometry, Geometry, Int, Int, Int) ⇒ Tile = {
    import geotrellis.vector.{Geometry => GTGeometry}
    (geom, bounds, value, cols, rows) ⇒ {
      // We have to do this because (as of spark 2.2.x) Encoder-only types
      // can't be used as UDF inputs. Only Spark-native types and UDTs.
      val extent = Extent(bounds.getEnvelopeInternal)
      GTGeometry(geom).rasterizeWithValue(RasterExtent(extent, cols, rows), value).tile
    }
  }

  /** Reporjects a geometry column from one CRS to another, where CRS are defined in Proj4 format. */
  private[rasterframes] val reprojectGeometryCRSName: (Geometry, String, String) ⇒ Geometry =
    (sourceGeom, srcName, dstName) ⇒ {
      val src = LazyCRS(srcName)
      val dst = LazyCRS(dstName)
      val trans = new ReprojectionTransformer(src, dst)
      trans.transform(sourceGeom)
    }

  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rf_make_constant_tile", makeConstantTile)
    sqlContext.udf.register("rf_make_zeros_tile", tileZeros)
    sqlContext.udf.register("rf_make_ones_tile", tileOnes)

    sqlContext.udf.register("rf_cell_types", cellTypes)
    sqlContext.udf.register("rf_rasterize", rasterize)

    sqlContext.udf.register("st_reproject", reprojectGeometryCRSName)
  }
}
