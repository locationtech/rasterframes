/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package astraea.spark.rasterframes

import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{Tile, _}
import geotrellis.raster.render.ascii.AsciiArtEncoder
import geotrellis.vector.Extent
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.gt.types

import scala.reflect.runtime.universe._

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


  /** Count tile cells that have a data value. */
  private[rasterframes] val dataCells: (Tile) ⇒ Long = safeEval((t: Tile) ⇒ {
    var count: Long = 0
    t.dualForeach(
      z ⇒ if(isData(z)) count = count + 1
    ) (
      z ⇒ if(isData(z)) count = count + 1
    )
    count
  })

  /** Count tile cells that have a no-data value. */
  private[rasterframes] val noDataCells: (Tile) ⇒ Long = safeEval((t: Tile) ⇒ {
    var count: Long = 0
    t.dualForeach(
      z ⇒ if(isNoData(z)) count = count + 1
    )(
      z ⇒ if(isNoData(z)) count = count + 1
    )
    count
  })

  /** Flattens tile into an array. */
  private[rasterframes] def tileToArray[T: HasCellType: TypeTag]: (Tile) ⇒ Array[T] = {
    def convert(tile: Tile) = {
      typeOf[T] match {
        case t if t =:= typeOf[Int] ⇒ tile.toArray()
        case t if t =:= typeOf[Double] ⇒ tile.toArrayDouble()
        case t if t =:= typeOf[Byte] ⇒ tile.toArray().map(_.toByte)          // TODO: Check NoData handling. probably need to use dualForeach
        case t if t =:= typeOf[Short] ⇒ tile.toArray().map(_.toShort)
        case t if t =:= typeOf[Float] ⇒ tile.toArrayDouble().map(_.toFloat)
      }
    }

    safeEval[Tile, Array[T]] { t ⇒
      val tile = t match {
        case c: ConstantTile ⇒ c.toArrayTile()
        case o ⇒ o
      }
      val asArray: Array[_] = tile match {
        case t: IntArrayTile ⇒
          if (typeOf[T] =:= typeOf[Int]) t.array
          else convert(t)
        case t: DoubleArrayTile ⇒
          if (typeOf[T] =:= typeOf[Double]) t.array
          else convert(t)
        case t: ByteArrayTile ⇒
          if (typeOf[T] =:= typeOf[Byte]) t.array
          else convert(t)
        case t: UByteArrayTile ⇒
          if (typeOf[T] =:= typeOf[Byte]) t.array
          else convert(t)
        case t: ShortArrayTile ⇒
          if (typeOf[T] =:= typeOf[Short]) t.array
          else convert(t)
        case t: UShortArrayTile ⇒
          if (typeOf[T] =:= typeOf[Short]) t.array
          else convert(t)
        case t: FloatArrayTile ⇒
          if (typeOf[T] =:= typeOf[Float]) t.array
          else convert(t)
        case _: Tile ⇒
          throw new IllegalArgumentException("Unsupported tile type: " + tile.getClass)
      }
      asArray.asInstanceOf[Array[T]]
    }
  }

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

  private[rasterframes] def assembleTile(cols: Int, rows: Int, ct: CellType) = TileAssemblerFunction(cols, rows, ct)

  /** Computes the column aggregate histogram */
  private[rasterframes] val aggHistogram = HistogramAggregateFunction()

  /** Computes the column aggregate statistics */
  private[rasterframes] val aggStats = CellStatsAggregateFunction()

  /** Change the tile's cell type. */
  private[rasterframes] def convertCellType(cellType: CellType) = safeEval[Tile, Tile](_.convert(cellType))

  /** Change the tile's cell type. */
  private[rasterframes] def convertCellType(cellTypeName: String) =
    safeEval[Tile, Tile](_.convert(CellType.fromName(cellTypeName)))

  /** Convert the cell type of Tile */
  private[rasterframes] val convertCellType: (Tile, String) ⇒ Tile = (t: Tile, s: String) ⇒ t.convert(CellType.fromName(s))

  /** Set the tile's no-data value. */
  private[rasterframes] def withNoData(nodata: Double) = safeEval[Tile, Tile](_.withNoData(Some(nodata)))

  /** Single tile histogram. */
  private[rasterframes] val tileHistogram = safeEval[Tile, CellHistogram](t ⇒ CellHistogram(t.histogramDouble))

  /** Single tile statistics. Convenience for `tileHistogram.statistics`. */
  private[rasterframes] val tileStats = safeEval[Tile, CellStatistics]((t: Tile) ⇒
    if (t.cellType.isFloatingPoint) t.statisticsDouble.map(CellStatistics.apply).orNull
    else t.statistics.map(CellStatistics.apply).orNull
  )

  /** Add up all the cell values. */
  private[rasterframes] val tileSum: (Tile) ⇒ Double = safeEval((t: Tile) ⇒ {
    var sum: Double = 0.0
    t.foreachDouble(z ⇒ if(isData(z)) sum = sum + z)
    sum
  })

  /** Find the minimum cell value. */
  private[rasterframes] val tileMin: (Tile) ⇒ Double = safeEval((t: Tile) ⇒ {
    var min: Double = Double.MaxValue
    t.foreachDouble(z ⇒ if(isData(z)) min = math.min(min, z))
    if (min == Double.MaxValue) Double.NaN
    else min
  })

  /** Find the maximum cell value. */
  private[rasterframes] val tileMax: (Tile) ⇒ Double = safeEval((t: Tile) ⇒ {
    var max: Double = Double.MinValue
    t.foreachDouble(z ⇒ if(isData(z)) max = math.max(max, z))
    if (max == Double.MinValue) Double.NaN
    else max
  })

  /** Single tile mean. Convenience for `tileHistogram.statistics.mean`. */
  private[rasterframes] val tileMean: (Tile) ⇒ Double = safeEval((t: Tile) ⇒ {
    var sum: Double = 0.0
    var count: Long = 0
    t.dualForeach(
      z ⇒ if(isData(z)) { count = count + 1; sum = sum + z }
    ) (
      z ⇒ if(isData(z)) { count = count + 1; sum = sum + z }
    )
    sum/count
  })

  /** Compute summary cell-wise statistics across tiles. */
  private[rasterframes] val localAggStats = new LocalStatsAggregateFunction()

  /** Compute the cell-wise max across tiles. */
  private[rasterframes] val localAggMax = new LocalTileOpAggregateFunction(Max)

  /** Compute the cell-wise min across tiles. */
  private[rasterframes] val localAggMin = new LocalTileOpAggregateFunction(Min)

  /** Compute the cell-wise main across tiles. */
  private[rasterframes] val localAggMean = new LocalMeanAggregateFunction()

  /** Compute the cell-wise count of non-NA across tiles. */
  private[rasterframes] val localAggCount = new LocalCountAggregateFunction(true)

  /** Compute the cell-wise count of non-NA across tiles. */
  private[rasterframes] val localAggNodataCount = new LocalCountAggregateFunction(false)

  /** Convert the tile to a floating point type as needed for scalar operations. */
  private def floatingPointTile(t: Tile) = if (t.cellType.isFloatingPoint) t else t.convert(DoubleConstantNoDataCellType)

  /** Cell-wise addition between tiles. */
  private[rasterframes] val localAdd: (Tile, Tile) ⇒ Tile = safeEval(Add.apply)

  /** Cell-wise addition of a scalar to a tile. */
  private[rasterframes] val localAddScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar:Int) => {
    t.localAdd(scalar)
  })

  /** Cell-wise addition of a scalar to a tile. */
  private[rasterframes] val localAddScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar:Double) => {
    floatingPointTile(t).localAdd(scalar)
  })

  /** Cell-wise subtraction between tiles. */
  private[rasterframes] val localSubtract: (Tile, Tile) ⇒ Tile = safeEval(Subtract.apply)

  /** Cell-wise subtraction of a scalar from a tile. */
  private[rasterframes] val localSubtractScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar:Int) => {
    t.localSubtract(scalar)
  })

  /** Cell-wise subtraction of a scalar from a tile. */
  private[rasterframes] val localSubtractScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar:Double) => {
    floatingPointTile(t).localSubtract(scalar)
  })

  /** Cell-wise multiplication between tiles. */
  private[rasterframes] val localMultiply: (Tile, Tile) ⇒ Tile = safeEval(Multiply.apply)

  /** Cell-wise multiplication of a tile by a scalar. */
  private[rasterframes] val localMultiplyScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar:Int) => {
    t.localMultiply(scalar)
  })

  /** Cell-wise multiplication of a tile by a scalar. */
  private[rasterframes] val localMultiplyScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar:Double) => {
    floatingPointTile(t).localMultiply(scalar)
  })

  /** Cell-wise division between tiles. */
  private[rasterframes] val localDivide: (Tile, Tile) ⇒ Tile = safeEval(Divide.apply)

  /** Cell-wise division of a tile by a scalar. */
  private[rasterframes] val localDivideScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar:Int) => {
    t.localDivide(scalar)
  })

  /** Cell-wise division of a tile by a scalar. */
  private[rasterframes] val localDivideScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar:Double) => {
    floatingPointTile(t).localDivide(scalar)
  })

  /** Cell-wise normalized difference of tiles. */
  private[rasterframes] val normalizedDifference:  (Tile, Tile) ⇒ Tile = safeEval((t1: Tile, t2:Tile) => {
    Divide(Subtract(t1, t2), Add(t1, t2))
  })

  /** Render tile as ASCII string. */
  private[rasterframes] val renderAscii: (Tile) ⇒ String = safeEval(_.renderAscii(AsciiArtEncoder.Palette.NARROW))

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
    * Generate a tile with the values from the data tile, but where cells in the
    * masking tile contain NODATA, replace the data value with NODATA.
    */
  private[rasterframes] val mask: (Tile, Tile) ⇒ Tile =
    (dataTile, maskingTile) ⇒ Mask(dataTile, Defined(maskingTile), 0, NODATA)

  /**
    * Generate a tile with the values from the data tile, but where cells in the
    * masking tile DO NOT contain NODATA, replace the data value with NODATA.
    */
  private[rasterframes] val inverseMask: (Tile, Tile) ⇒ Tile =
    (dataTile, maskingTile) ⇒ InverseMask(dataTile, Defined(maskingTile), 0, NODATA)

  /**
   * Rasterize geometry into tiles.
   */
  private[rasterframes] val rasterize: (Geometry, Geometry, Int, Int, Int) ⇒ Tile = {
    import geotrellis.vector.{Geometry ⇒ GTGeometry}
    (geom, bounds, value, cols, rows) ⇒ {
      // We have to do this because (as of spark 2.2.x) Encoder-only types
      // can't be used as UDF inputs. Only Spark-native types and UDTs.
      val extent = Extent(bounds.getEnvelopeInternal)
      GTGeometry(geom).rasterizeWithValue(RasterExtent(extent, cols, rows), value)
    }
  }

  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rf_mask", mask)
    sqlContext.udf.register("rf_inverseMask", inverseMask)
    sqlContext.udf.register("rf_makeConstantTile", makeConstantTile)
    sqlContext.udf.register("rf_tileZeros", tileZeros)
    sqlContext.udf.register("rf_tileOnes", tileOnes)
    sqlContext.udf.register("rf_tileToArrayInt", tileToArray[Int])
    sqlContext.udf.register("rf_tileToArrayDouble", tileToArray[Double])
    sqlContext.udf.register("rf_aggHistogram", aggHistogram)
    sqlContext.udf.register("rf_aggStats", aggStats)
    sqlContext.udf.register("rf_tileMin", tileMean)
    sqlContext.udf.register("rf_tileMax", tileMean)
    sqlContext.udf.register("rf_tileMean", tileMean)
    sqlContext.udf.register("rf_tileSum", tileSum)
    sqlContext.udf.register("rf_tileHistogram", tileHistogram)
    sqlContext.udf.register("rf_tileStats", tileStats)
    sqlContext.udf.register("rf_dataCells", dataCells)
    sqlContext.udf.register("rf_noDataCells", dataCells)
    sqlContext.udf.register("rf_localAggStats", localAggStats)
    sqlContext.udf.register("rf_localAggMax", localAggMax)
    sqlContext.udf.register("rf_localAggMin", localAggMin)
    sqlContext.udf.register("rf_localAggMean", localAggMean)
    sqlContext.udf.register("rf_localAggCount", localAggCount)
    sqlContext.udf.register("rf_localAdd", localAdd)
    sqlContext.udf.register("rf_localAddScalar", localAddScalar)
    sqlContext.udf.register("rf_localAddScalarInt", localAddScalarInt)
    sqlContext.udf.register("rf_localSubtract", localSubtract)
    sqlContext.udf.register("rf_localSubtractScalar", localSubtractScalar)
    sqlContext.udf.register("rf_localSubtractScalarInt", localSubtractScalarInt)
    sqlContext.udf.register("rf_localMultiply", localMultiply)
    sqlContext.udf.register("rf_localMultiplyScalar", localMultiplyScalar)
    sqlContext.udf.register("rf_localMultiplyScalarInt", localMultiplyScalarInt)
    sqlContext.udf.register("rf_localDivide", localDivide)
    sqlContext.udf.register("rf_localDivideScalar", localDivideScalar)
    sqlContext.udf.register("rf_localDivideScalarInt", localDivideScalarInt)
    sqlContext.udf.register("rf_normalizedDifference", normalizedDifference)
    sqlContext.udf.register("rf_cellTypes", cellTypes)
    sqlContext.udf.register("rf_renderAscii", renderAscii)
    sqlContext.udf.register("rf_convertCellType", convertCellType)
    sqlContext.udf.register("rf_rasterize", rasterize)
  }
}
