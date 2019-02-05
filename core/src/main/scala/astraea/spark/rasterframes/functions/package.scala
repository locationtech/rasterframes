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

import astraea.spark.rasterframes.jts.ReprojectionTransformer
import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import astraea.spark.rasterframes.util.CRSParser
import com.vividsolutions.jts.geom.Geometry
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.render.ascii.AsciiArtEncoder
import geotrellis.raster.{Tile, _}
import geotrellis.vector.Extent
import org.apache.spark.sql.SQLContext

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

  private[rasterframes] val isNoDataTile: (Tile) ⇒ Boolean = (t: Tile) ⇒ {
    if(t == null) true
    else t.isNoDataTile
  }

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

  /** Computes the column aggregate histogram */
  private[rasterframes] val aggHistogram = HistogramAggregate()

  /** Computes the column aggregate statistics */
  private[rasterframes] val aggStats = CellStatsAggregate()

  /** Set the tile's no-data value. */
  private[rasterframes] def withNoData(nodata: Double) = safeEval[Tile, Tile](_.withNoData(Some(nodata)))

  /** Single tile histogram. */
  private[rasterframes] val tileHistogram = safeEval[Tile, CellHistogram](t ⇒ CellHistogram(t.histogramDouble))

  /** Single tile statistics. Convenience for `tile_histogram.statistics`. */
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

  /** Single tile mean. Convenience for `tile_histogram.statistics.mean`. */
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
  private[rasterframes] val localAggStats = new LocalStatsAggregate()

  /** Compute the cell-wise max across tiles. */
  private[rasterframes] val localAggMax = new LocalTileOpAggregate(Max)

  /** Compute the cell-wise min across tiles. */
  private[rasterframes] val localAggMin = new LocalTileOpAggregate(Min)

  /** Compute the cell-wise main across tiles. */
  private[rasterframes] val localAggMean = new LocalMeanAggregate()

  /** Compute the cell-wise count of non-NA across tiles. */
  private[rasterframes] val localAggCount = new LocalCountAggregate(true)

  /** Compute the cell-wise count of non-NA across tiles. */
  private[rasterframes] val localAggNodataCount = new LocalCountAggregate(false)

  /** Convert the tile to a floating point type as needed for scalar operations. */
  @inline
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
    val diff = floatingPointTile(Subtract(t1, t2))
    val sum = floatingPointTile(Add(t1, t2))
    Divide(diff, sum)
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
    * masking tile contain the masking value, replace the data value with NODATA.
    */
  private[rasterframes] val maskByValue: (Tile, Tile, Int) ⇒ Tile =
    (dataTile, maskingTile, maskingValue) ⇒
      Mask(dataTile, maskingTile, maskingValue, NODATA)

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
      GTGeometry(geom).rasterizeWithValue(RasterExtent(extent, cols, rows), value).tile
    }
  }

  /** Cellwise less than value comparison between two tiles. */
  private[rasterframes] val localLess: (Tile, Tile) ⇒ Tile = safeEval(Less.apply)

  /** Cellwise less than value comparison between a tile and a scalar. */
  private[rasterframes] val localLessScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar: Int) ⇒ {
    t.localLess(scalar)
  })

  /** Cellwise less than value comparison between a tile and a scalar. */
  private[rasterframes] val localLessScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar: Double) ⇒ {
    floatingPointTile(t).localLess(scalar)
  })

  /** Cellwise less than or equal to value comparison between two tiles. */
  private[rasterframes] val localLessEqual: (Tile, Tile) ⇒ Tile = safeEval(LessOrEqual.apply)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  private[rasterframes] val localLessEqualScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar: Int) ⇒ {
    t.localLessOrEqual(scalar)
  })

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  private[rasterframes] val localLessEqualScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar: Double) ⇒ {
    floatingPointTile(t).localLessOrEqual(scalar)
  })

  /** Cellwise greater than value comparison between two tiles. */
  private[rasterframes] val localGreater: (Tile, Tile) ⇒ Tile = safeEval(Less.apply)

  /** Cellwise greater than value comparison between a tile and a scalar. */
  private[rasterframes] val localGreaterScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar: Int) ⇒ {
    t.localGreater(scalar)
  })

  /** Cellwise greater than value comparison between a tile and a scalar. */
  private[rasterframes] val localGreaterScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar: Double) ⇒ {
    floatingPointTile(t).localGreater(scalar)
  })

  /** Cellwise greater than or equal to value comparison between two tiles. */
  private[rasterframes] val localGreaterEqual: (Tile, Tile) ⇒ Tile = safeEval(LessOrEqual.apply)

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  private[rasterframes] val localGreaterEqualScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar: Int) ⇒ {
    t.localGreaterOrEqual(scalar)
  })

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  private[rasterframes] val localGreaterEqualScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar: Double) ⇒ {
    floatingPointTile(t).localGreaterOrEqual(scalar)
  })

  /** Cellwise equal to value comparison between two tiles. */
  private[rasterframes] val localEqual: (Tile, Tile) ⇒ Tile = safeEval(Equal.apply)

  /** Cellwise equal to value comparison between a tile and a scalar. */
  private[rasterframes] val localEqualScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar: Int) ⇒ {
    t.localEqual(scalar)
  })

  /** Cellwise equal to value comparison between a tile and a scalar. */
  private[rasterframes] val localEqualScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar: Double) ⇒ {
    floatingPointTile(t).localEqual(scalar)
  })

  /** Cellwise inequality value comparison between two tiles. */
  private[rasterframes] val localUnequal: (Tile, Tile) ⇒ Tile = safeEval(Unequal.apply)

  /** Cellwise inequality value comparison between a tile and a scalar. */
  private[rasterframes] val localUnequalScalarInt: (Tile, Int) ⇒ Tile = safeEval((t: Tile, scalar: Int) ⇒ {
    t.localUnequal(scalar)
  })

  /** Cellwise inequality value comparison between a tile and a scalar. */
  private[rasterframes] val localUnequalScalar: (Tile, Double) ⇒ Tile = safeEval((t: Tile, scalar: Double) ⇒ {
    floatingPointTile(t).localUnequal(scalar)
  })

  /** Reporjects a geometry column from one CRS to another. */
  private[rasterframes] val reprojectGeometry: (Geometry, CRS, CRS) ⇒ Geometry =
    (sourceGeom, src, dst) ⇒ {
      val trans = new ReprojectionTransformer(src, dst)
      trans.transform(sourceGeom)
    }

  /** Reporjects a geometry column from one CRS to another, where CRS are defined in Proj4 format. */
  private[rasterframes] val reprojectGeometryCRSName: (Geometry, String, String) ⇒ Geometry =
    (sourceGeom, srcName, dstName) ⇒ {
      val src = CRSParser(srcName)
      val dst = CRSParser(dstName)
      val trans = new ReprojectionTransformer(src, dst)
      trans.transform(sourceGeom)
    }

  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rf_mask", mask)
    sqlContext.udf.register("rf_mask_by_value", maskByValue)
    sqlContext.udf.register("rf_inverse_mask", inverseMask)
    sqlContext.udf.register("rf_make_constant_tile", makeConstantTile)
    sqlContext.udf.register("rf_tile_zeros", tileZeros)
    sqlContext.udf.register("rf_tile_ones", tileOnes)
    sqlContext.udf.register("rf_tile_to_array_int", tileToArray[Int])
    sqlContext.udf.register("rf_tile_to_array_double", tileToArray[Double])
    sqlContext.udf.register("rf_agg_histogram", aggHistogram)
    sqlContext.udf.register("rf_agg_stats", aggStats)
    sqlContext.udf.register("rf_tile_min", tileMin)
    sqlContext.udf.register("rf_tile_max", tileMax)
    sqlContext.udf.register("rf_tile_mean", tileMean)
    sqlContext.udf.register("rf_tile_sum", tileSum)
    sqlContext.udf.register("rf_tile_histogram", tileHistogram)
    sqlContext.udf.register("rf_tile_stats", tileStats)
    sqlContext.udf.register("rf_data_cells", dataCells)
    sqlContext.udf.register("rf_no_data_cells", noDataCells)
    sqlContext.udf.register("rf_is_no_data_tile", isNoDataTile)
    sqlContext.udf.register("rf_local_agg_stats", localAggStats)
    sqlContext.udf.register("rf_local_agg_max", localAggMax)
    sqlContext.udf.register("rf_local_agg_min", localAggMin)
    sqlContext.udf.register("rf_local_agg_mean", localAggMean)
    sqlContext.udf.register("rf_local_agg_count", localAggCount)
    sqlContext.udf.register("rf_local_add", localAdd)
    sqlContext.udf.register("rf_local_add_scalar", localAddScalar)
    sqlContext.udf.register("rf_local_add_scalar_int", localAddScalarInt)
    sqlContext.udf.register("rf_local_subtract", localSubtract)
    sqlContext.udf.register("rf_local_subtract_scalar", localSubtractScalar)
    sqlContext.udf.register("rf_local_subtract_scalar_int", localSubtractScalarInt)
    sqlContext.udf.register("rf_local_multiply", localMultiply)
    sqlContext.udf.register("rf_local_multiply_scalar", localMultiplyScalar)
    sqlContext.udf.register("rf_local_multiply_scalar_int", localMultiplyScalarInt)
    sqlContext.udf.register("rf_local_divide", localDivide)
    sqlContext.udf.register("rf_local_divide_scalar", localDivideScalar)
    sqlContext.udf.register("rf_local_divide_scalar_int", localDivideScalarInt)
    sqlContext.udf.register("rf_normalized_difference", normalizedDifference)
    sqlContext.udf.register("rf_cell_types", cellTypes)
    sqlContext.udf.register("rf_render_ascii", renderAscii)
    sqlContext.udf.register("rf_rasterize", rasterize)
    sqlContext.udf.register("rf_less", localLess)
    sqlContext.udf.register("rf_less_scalar", localLessScalar)
    sqlContext.udf.register("rf_less_scalar_int", localLessScalarInt)
    sqlContext.udf.register("rf_less_equal", localLessEqual)
    sqlContext.udf.register("rf_less_equal_scalar", localLessEqualScalar)
    sqlContext.udf.register("rf_less_equal_scalar_int", localLessEqualScalarInt)
    sqlContext.udf.register("rf_greater", localGreater)
    sqlContext.udf.register("rf_greater_scalar", localGreaterScalar)
    sqlContext.udf.register("rf_greaterscalar_int", localGreaterScalarInt)
    sqlContext.udf.register("rf_greater_equal", localGreaterEqual)
    sqlContext.udf.register("rf_greater_equal_scalar", localGreaterEqualScalar)
    sqlContext.udf.register("rf_greater_equal_scalar_int", localGreaterEqualScalarInt)
    sqlContext.udf.register("rf_equal", localEqual)
    sqlContext.udf.register("rf_equal_scalar", localEqualScalar)
    sqlContext.udf.register("rf_equal_scalar_int", localEqualScalarInt)
    sqlContext.udf.register("rf_unequal", localUnequal)
    sqlContext.udf.register("rf_unequal_scalar", localUnequalScalar)
    sqlContext.udf.register("rf_unequal_scalar_int", localUnequalScalarInt)
    sqlContext.udf.register("rf_reproject_geometry", reprojectGeometryCRSName)
  }
}
