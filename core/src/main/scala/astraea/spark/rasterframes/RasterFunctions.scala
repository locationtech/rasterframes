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

package astraea.spark.rasterframes

import astraea.spark.rasterframes.encoders.SparkDefaultEncoders
import astraea.spark.rasterframes.expressions.ExplodeTileExpression
import astraea.spark.rasterframes.functions.{CellCountAggregateFunction, CellMeanAggregateFunction}
import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import astraea.spark.rasterframes.{functions ⇒ F}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.rf._

import scala.reflect.runtime.universe._

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @since 4/3/17
 */
trait RasterFunctions {
  import util._
  import SparkDefaultEncoders._

  // format: off
  /** Create a row for each cell in Tile. */
  def explodeTiles(cols: Column*): Column = explodeTileSample(1.0, cols: _*)

  /** Create a row for each cell in Tile with random sampling. */
  def explodeTileSample(sampleFraction: Double, cols: Column*): Column = {
    val exploder = ExplodeTileExpression(sampleFraction, cols.map(_.expr))
    // Hack to grab the first two non-cell columns, containing the column and row indexes
    val metaNames = exploder.elementSchema.fieldNames.take(2)
    val colNames = cols.map(_.columnName)
    new Column(exploder).as(metaNames ++ colNames)
  }

  /** Query the number of (cols, rows) in a Tile. */
  def tileDimensions(col: Column): Column = expressions.DimensionsExpression(col.expr).asColumn

  /** Extracts the bounding box of a geometry as a JTS envelope. */
  def envelope(col: Column): TypedColumn[Any, Envelope] = expressions.EnvelopeExpression(col.expr).asColumn.as[Envelope]

  /** Flattens Tile into an array. A numeric type parameter is required. */
  @Experimental
  def tileToArray[T: HasCellType: TypeTag](col: Column): TypedColumn[Any, Array[T]] = withAlias("tileToArray", col)(
    udf[Array[T], Tile](F.tileToArray).apply(col)
  ).as[Array[T]]

  @Experimental
  /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
  def arrayToTile(arrayCol: Column, cols: Int, rows: Int) = withAlias("arrayToTile", arrayCol)(
    udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
  )

  /** Create a Tile from  a column of cell data with location indexes. */
  @Experimental
  def assembleTile(columnIndex: Column, rowIndex: Column, cellData: Column, cols: Int, rows: Int, ct: CellType): TypedColumn[Any, Tile] = {
    F.assembleTile(cols, rows, ct)(columnIndex, rowIndex, cellData)
  }.as(cellData.columnName).as[Tile]

  /** Extract the Tile's cell type */
  def cellType(col: Column): TypedColumn[Any, String] =
    expressions.CellTypeExpression(col.expr).asColumn.as[String]

  /** Change the Tile's cell type */
  def convertCellType(col: Column, cellType: CellType): TypedColumn[Any, Tile] =
    udf[Tile, Tile](F.convertCellType(cellType)).apply(col).as[Tile]

  /** Change the Tile's cell type */
  def convertCellType(col: Column, cellTypeName: String): TypedColumn[Any, Tile] =
    udf[Tile, Tile](F.convertCellType(cellTypeName)).apply(col).as[Tile]

  /** Assign a `NoData` value to the Tiles. */
  def withNoData(col: Column, nodata: Double) = withAlias("withNoData", col)(
    udf[Tile, Tile](F.withNoData(nodata)).apply(col)
  ).as[Tile]

  /**  Compute the full column aggregate floating point histogram. */
  def aggHistogram(col: Column): TypedColumn[Any, CellHistogram] =
  withAlias("histogram", col)(
    F.aggHistogram(col)
  ).as[CellHistogram]

  /** Compute the full column aggregate floating point statistics. */
  def aggStats(col: Column): TypedColumn[Any, CellStatistics] = withAlias("aggStats", col)(
    F.aggStats(col)
  ).as[CellStatistics]

  /** Computes the column aggregate mean. */
  def aggMean(col: Column) = CellMeanAggregateFunction(col.expr)
    .toAggregateExpression().asColumn
    .as[Double]

  /** Computes the number of non-NoData cells in a column. */
  def aggDataCells(col: Column) = CellCountAggregateFunction(true, col.expr)
    .toAggregateExpression().asColumn
    .as[Long]

  /** Computes the number of NoData cells in a column. */
  def aggNoDataCells(col: Column) = CellCountAggregateFunction(false, col.expr)
    .toAggregateExpression().asColumn
    .as[Long]

  /** Compute the Tile-wise mean */
  def tileMean(col: Column): TypedColumn[Any, Double] =
  withAlias("tileMean", col)(
    udf[Double, Tile](F.tileMean).apply(col)
  ).as[Double]

  /** Compute the Tile-wise sum */
  def tileSum(col: Column): TypedColumn[Any, Double] =
  withAlias("tileSum", col)(
    udf[Double, Tile](F.tileSum).apply(col)
  ).as[Double]

  /** Compute the minimum cell value in tile. */
  def tileMin(col: Column): TypedColumn[Any, Double] =
  withAlias("tileMin", col)(
    udf[Double, Tile](F.tileMin).apply(col)
  ).as[Double]

  /** Compute the maximum cell value in tile. */
  def tileMax(col: Column): TypedColumn[Any, Double] =
  withAlias("tileMax", col)(
    udf[Double, Tile](F.tileMax).apply(col)
  ).as[Double]

  /** Compute TileHistogram of Tile values. */
  def tileHistogram(col: Column): TypedColumn[Any, CellHistogram] =
  withAlias("tileHistogram", col)(
    udf[CellHistogram, Tile](F.tileHistogram).apply(col)
  ).as[CellHistogram]

  /** Compute statistics of Tile values. */
  def tileStats(col: Column): TypedColumn[Any, CellStatistics] =
  withAlias("tileStats", col)(
    udf[CellStatistics, Tile](F.tileStats).apply(col)
  ).as[CellStatistics]

  /** Counts the number of non-NoData cells per Tile. */
  def dataCells(tile: Column): TypedColumn[Any, Long] =
    withAlias("dataCells", tile)(
      udf(F.dataCells).apply(tile)
    ).as[Long]

  /** Counts the number of NoData cells per Tile. */
  def noDataCells(tile: Column): TypedColumn[Any, Long] =
    withAlias("noDataCells", tile)(
      udf(F.noDataCells).apply(tile)
    ).as[Long]

  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  def localAggStats(col: Column): Column =
  withAlias("localAggStats", col)(
    F.localAggStats(col)
  )

  /** Compute the cell-wise/local max operation between Tiles in a column. */
  def localAggMax(col: Column): TypedColumn[Any, Tile] =
  withAlias("localAggMax", col)(
    F.localAggMax(col)
  ).as[Tile]

  /** Compute the cellwise/local min operation between Tiles in a column. */
  def localAggMin(col: Column): TypedColumn[Any, Tile] =
  withAlias("localAggMin", col)(
    F.localAggMin(col)
  ).as[Tile]

  /** Compute the cellwise/local mean operation between Tiles in a column. */
  def localAggMean(col: Column): TypedColumn[Any, Tile] =
  withAlias("localAggMean", col)(
    F.localAggMean(col)
  ).as[Tile]

  /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
  def localAggDataCells(col: Column): TypedColumn[Any, Tile] =
  withAlias("localCount", col)(
    F.localAggCount(col)
  ).as[Tile]

  /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
  def localAggNoDataCells(col: Column): TypedColumn[Any, Tile] =
  withAlias("localNodataCount", col)(
    F.localAggNodataCount(col)
  ).as[Tile]

  /** Cellwise addition between two Tiles. */
  def localAdd(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localAdd", left, right)(
    udf(F.localAdd).apply(left, right)
  ).as[Tile]

  /** Cellwise addition of a scalar to a tile. */
  def localAddScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match {
      case i: Int => F.localAddScalarInt(_: Tile, i)
      case d: Double => F.localAddScalar(_: Tile, d)
    }

    udf(f).apply(tileCol).as(s"localAddScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise subtraction between two Tiles. */
  def localSubtract(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localSubtract", left, right)(
    udf(F.localSubtract).apply(left, right)
  ).as[Tile]

  /** Cellwise subtraction of a scalar from a tile. */
  def localSubtractScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match {
      case i: Int => F.localSubtractScalarInt(_: Tile, i)
      case d: Double => F.localSubtractScalar(_: Tile, d)
    }

    udf(f).apply(tileCol).as(s"localSubtractScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise multiplication between two Tiles. */
  def localMultiply(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localMultiply", left, right)(
    udf(F.localMultiply).apply(left, right)
  ).as[Tile]

  /** Cellwise multiplication of a tile by a scalar. */
  def localMultiplyScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match {
      case i: Int => F.localMultiplyScalarInt(_: Tile, i)
      case d: Double => F.localMultiplyScalar(_: Tile, d)
    }

    udf(f).apply(tileCol).as(s"localMultiplyScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise division between two Tiles. */
  def localDivide(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localDivide", left, right)(
    udf(F.localDivide).apply(left, right)
  ).as[Tile]

  /** Cellwise division of a tile by a scalar. */
  def localDivideScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match {
      case i: Int => F.localDivideScalarInt(_: Tile, i)
      case d: Double => F.localDivideScalar(_: Tile, d)
    }

    udf(f).apply(tileCol).as(s"localDivideScalar($tileCol, $value)").as[Tile]
  }

  /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
  def localAlgebra(op: LocalTileBinaryOp, left: Column, right: Column):
  TypedColumn[Any, Tile] =
    withAlias(opName(op), left, right)(
      udf[Tile, Tile, Tile](op.apply).apply(left, right)
    ).as[Tile]

  /** Compute the normalized difference of two tile columns */
  def normalizedDifference(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("normalizedDifference", left, right)(
      udf(F.normalizedDifference).apply(left, right)
    ).as[Tile]

  /** Constructor for constant tile column */
  def makeConstantTile(value: Number, cols: Int, rows: Int, cellType: String): TypedColumn[Any, Tile] =
    udf(() => F.makeConstantTile(value, cols, rows, cellType)).apply().as(s"constant_$cellType").as[Tile]

  /** Alias for column of constant tiles of zero */
  def tileZeros(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
    udf(() => F.tileZeros(cols, rows, cellType)).apply().as(s"zeros_$cellType").as[Tile]

  /** Alias for column of constant tiles of one */
  def tileOnes(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
    udf(() => F.tileOnes(cols, rows, cellType)).apply().as(s"ones_$cellType").as[Tile]

  /** Where the mask tile contains NODATA, replace values in the source tile with NODATA */
  def mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    withAlias("mask", sourceTile, maskTile)(
      udf(F.mask).apply(sourceTile, maskTile)
    ).as[Tile]

  /** Where the mask tile DOES NOT contain NODATA, replace values in the source tile with NODATA */
  def inverseMask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    withAlias("inverseMask", sourceTile, maskTile)(
      udf(F.inverseMask).apply(sourceTile, maskTile)
    ).as[Tile]

  /** Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value. */
  def rasterize(geometry: Column, bounds: Column, value: Column, cols: Int, rows: Int): TypedColumn[Any, Tile] =
    withAlias("rasterize", geometry)(
      udf(F.rasterize(_: Geometry, _: Geometry, _: Int, cols, rows)).apply(geometry, bounds, value)
    ).as[Tile]

  /** Reproject a column of geometry from one CRS to another. */  
  def reprojectGeometry(sourceGeom: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
    withAlias("reprojectGeometry", sourceGeom)(
      udf(F.reprojectGeometry(_: Geometry, srcCRS, dstCRS)).apply(sourceGeom)
    ).as[Geometry]

  /** Render Tile as ASCII string for debugging purposes. */
  @Experimental
  def renderAscii(col: Column): TypedColumn[Any, String] =
  withAlias("renderAscii", col)(
    udf[String, Tile](F.renderAscii).apply(col)
  ).as[String]

  /** Cellwise less than value comparison between two tiles. */
  def localLess(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("localLess", left, right)(
      udf(F.localLess).apply(left, right)
    ).as[Tile]


  /** Cellwise less than value comparison between a tile and a scalar. */
  def localLessScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localLessScalarInt(_: Tile, i)
      case d: Double ⇒ F.localLessScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"localLessScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def localLessEqual(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("localLessEqual", left, right)(
      udf(F.localLess).apply(left, right)
    ).as[Tile]

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def localLessEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localLessEqualScalarInt(_: Tile, i)
      case d: Double ⇒ F.localLessEqualScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"localLessEqualScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise greater than value comparison between two tiles. */
  def localGreater(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("localGreater", left, right)(
      udf(F.localGreater).apply(left, right)
    ).as[Tile]


  /** Cellwise greater than value comparison between a tile and a scalar. */
  def localGreaterScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localGreaterScalarInt(_: Tile, i)
      case d: Double ⇒ F.localGreaterScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"localGreaterScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise greater than or equal to value comparison between two tiles. */
  def localGreaterEqual(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("localGreaterEqual", left, right)(
      udf(F.localGreaterEqual).apply(left, right)
    ).as[Tile]

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  def localGreaterEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localGreaterEqualScalarInt(_: Tile, i)
      case d: Double ⇒ F.localGreaterEqualScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"localGreaterEqualScalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise equal to value comparison between two tiles. */
  def localEqual(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("localEqual", left, right)(
      udf(F.localEqual).apply(left, right)
    ).as[Tile]

  /** Cellwise equal to value comparison between a tile and a scalar. */
  def localEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localEqualScalarInt(_: Tile, i)
      case d: Double ⇒ F.localEqualScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"localEqualScalar($tileCol, $value)").as[Tile]
  }
  /** Cellwise inequality comparison between two tiles. */
  def localUnequal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("localUnequal", left, right)(
      udf(F.localUnequal).apply(left, right)
    ).as[Tile]

  /** Cellwise inequality comparison between a tile and a scalar. */
  def localUnequalScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localUnequalScalarInt(_: Tile, i)
      case d: Double ⇒ F.localUnequalScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"localUnequalScalar($tileCol, $value)").as[Tile]
  }
}
