/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

package astraea.spark.rasterframes.util
import astraea.spark.rasterframes.encoders.SparkDefaultEncoders
import astraea.spark.rasterframes.expressions.BinaryRasterOp
import astraea.spark.rasterframes.functions.{CellCountAggregate, CellMeanAggregate}
import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import astraea.spark.rasterframes.{HasCellType, expressions => E, functions => F}
import com.vividsolutions.jts.geom.Geometry
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.rf.VersionShims
import org.apache.spark.sql.{Column, SQLContext, TypedColumn, rf}

import scala.reflect.runtime.universe._

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @since 4/3/17
 */
object ZeroSevenCompatibilityKit {
  import SparkDefaultEncoders._

  trait RasterFunctions {
    // format: off
    /** Create a row for each cell in Tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def explodeTiles(cols: Column*): Column = explodeTilesSample(1.0, None, cols: _*)

    /** Create a row for each cell in Tile with random sampling and optional seed. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def explodeTilesSample(sampleFraction: Double, seed: Option[Long], cols: Column*): Column =
      E.ExplodeTiles(sampleFraction, seed, cols)

    /** Create a row for each cell in Tile with random sampling (no seed). */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def explodeTilesSample(sampleFraction: Double, cols: Column*): Column =
      E.ExplodeTiles(sampleFraction, None, cols)

    /** Query the number of (cols, rows) in a Tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileDimensions(col: Column): Column = E.GetDimensions(col)

    /** Flattens Tile into an array. A numeric type parameter is required. */
    @Experimental
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileToArray[T: HasCellType: TypeTag](col: Column): TypedColumn[Any, Array[T]] = withAlias("tileToArray", col)(
      udf[Array[T], Tile](F.tileToArray).apply(col)
    ).as[Array[T]]

    @Experimental
    /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def arrayToTile(arrayCol: Column, cols: Int, rows: Int) = withAlias("array_to_tile", arrayCol)(
      udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
    )

    /** Create a Tile from a column of cell data with location indexes and preform cell conversion. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def assembleTile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Int, tileRows: Int, ct: CellType): TypedColumn[Any, Tile] =
      convertCellType(F.TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows)), ct).as(cellData.columnName).as[Tile]

    /** Create a Tile from  a column of cell data with location indexes. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def assembleTile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Column, tileRows: Column): TypedColumn[Any, Tile] =
      F.TileAssembler(columnIndex, rowIndex, cellData, tileCols, tileRows)

    /** Extract the Tile's cell type */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def cellType(col: Column): TypedColumn[Any, CellType] = E.GetCellType(col)

    /** Change the Tile's cell type */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def convertCellType(col: Column, cellType: CellType): TypedColumn[Any, Tile] =
      E.SetCellType(col, cellType)

    /** Change the Tile's cell type */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def convertCellType(col: Column, cellTypeName: String): TypedColumn[Any, Tile] =
      E.SetCellType(col, cellTypeName)

    /** Convert a bounding box structure to a Geometry type. Intented to support multiple schemas. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def boundsGeometry(bounds: Column): TypedColumn[Any, Geometry] = E.BoundsToGeometry(bounds)

    /** Assign a `NoData` value to the Tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def withNoData(col: Column, nodata: Double) = withAlias("withNoData", col)(
      udf[Tile, Tile](F.withNoData(nodata)).apply(col)
    ).as[Tile]

    /**  Compute the full column aggregate floating point histogram. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggHistogram(col: Column): TypedColumn[Any, CellHistogram] =
    withAlias("histogram", col)(
      F.aggHistogram(col)
    ).as[CellHistogram]

    /** Compute the full column aggregate floating point statistics. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggStats(col: Column): TypedColumn[Any, CellStatistics] = withAlias("aggStats", col)(
      F.aggStats(col)
    ).as[CellStatistics]

    /** Computes the column aggregate mean. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggMean(col: Column) = CellMeanAggregate(col)

    /** Computes the number of non-NoData cells in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggDataCells(col: Column) = CellCountAggregate(true, col)

    /** Computes the number of NoData cells in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggNoDataCells(col: Column) = CellCountAggregate(false, col)

    /** Compute the Tile-wise mean */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileMean(col: Column): TypedColumn[Any, Double] =
    withAlias("tileMean", col)(
      udf[Double, Tile](F.tileMean).apply(col)
    ).as[Double]

    /** Compute the Tile-wise sum */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileSum(col: Column): TypedColumn[Any, Double] =
    withAlias("tileSum", col)(
      udf[Double, Tile](F.tileSum).apply(col)
    ).as[Double]

    /** Compute the minimum cell value in tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileMin(col: Column): TypedColumn[Any, Double] =
    withAlias("tileMin", col)(
      udf[Double, Tile](F.tileMin).apply(col)
    ).as[Double]

    /** Compute the maximum cell value in tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileMax(col: Column): TypedColumn[Any, Double] =
    withAlias("tileMax", col)(
      udf[Double, Tile](F.tileMax).apply(col)
    ).as[Double]

    /** Compute TileHistogram of Tile values. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileHistogram(col: Column): TypedColumn[Any, CellHistogram] =
    withAlias("tileHistogram", col)(
      udf[CellHistogram, Tile](F.tileHistogram).apply(col)
    ).as[CellHistogram]

    /** Compute statistics of Tile values. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileStats(col: Column): TypedColumn[Any, CellStatistics] =
    withAlias("tileStats", col)(
      udf[CellStatistics, Tile](F.tileStats).apply(col)
    ).as[CellStatistics]

    /** Counts the number of non-NoData cells per Tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def dataCells(tile: Column): TypedColumn[Any, Long] =
      withAlias("dataCells", tile)(
        udf(F.dataCells).apply(tile)
      ).as[Long]

    /** Counts the number of NoData cells per Tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def noDataCells(tile: Column): TypedColumn[Any, Long] =
      withAlias("noDataCells", tile)(
        udf(F.noDataCells).apply(tile)
      ).as[Long]


    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def isNoDataTile(tile: Column): TypedColumn[Any, Boolean] =
      withAlias("isNoDataTile", tile)(
        udf(F.isNoDataTile).apply(tile)
      ).as[Boolean]

    /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggStats(col: Column): Column =
    withAlias("localAggStats", col)(
      F.localAggStats(col)
    )

    /** Compute the cell-wise/local max operation between Tiles in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggMax(col: Column): TypedColumn[Any, Tile] =
    withAlias("localAggMax", col)(
      F.localAggMax(col)
    ).as[Tile]

    /** Compute the cellwise/local min operation between Tiles in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggMin(col: Column): TypedColumn[Any, Tile] =
    withAlias("localAggMin", col)(
      F.localAggMin(col)
    ).as[Tile]

    /** Compute the cellwise/local mean operation between Tiles in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggMean(col: Column): TypedColumn[Any, Tile] =
    withAlias("localAggMean", col)(
      F.localAggMean(col)
    ).as[Tile]

    /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggDataCells(col: Column): TypedColumn[Any, Tile] =
    withAlias("localCount", col)(
      F.localAggCount(col)
    ).as[Tile]

    /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggNoDataCells(col: Column): TypedColumn[Any, Tile] =
    withAlias("localNodataCount", col)(
      F.localAggNodataCount(col)
    ).as[Tile]

    /** Cellwise addition between two Tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAdd(left: Column, right: Column): Column = BinaryRasterOp.Add(left, right)

    /** Cellwise addition of a scalar to a tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAddScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match {
        case i: Int => F.localAddScalarInt(_: Tile, i)
        case d: Double => F.localAddScalar(_: Tile, d)
      }

      udf(f).apply(tileCol).as(s"local_add_scalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise subtraction between two Tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localSubtract(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("localSubtract", left, right)(
      udf(F.localSubtract).apply(left, right)
    ).as[Tile]

    /** Cellwise subtraction of a scalar from a tile. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localSubtractScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match {
        case i: Int => F.localSubtractScalarInt(_: Tile, i)
        case d: Double => F.localSubtractScalar(_: Tile, d)
      }

      udf(f).apply(tileCol).as(s"localSubtractScalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise multiplication between two Tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localMultiply(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("localMultiply", left, right)(
      udf(F.localMultiply).apply(left, right)
    ).as[Tile]

    /** Cellwise multiplication of a tile by a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localMultiplyScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match {
        case i: Int => F.localMultiplyScalarInt(_: Tile, i)
        case d: Double => F.localMultiplyScalar(_: Tile, d)
      }

      udf(f).apply(tileCol).as(s"localMultiplyScalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise division between two Tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localDivide(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("localDivide", left, right)(
      udf(F.localDivide).apply(left, right)
    ).as[Tile]

    /** Cellwise division of a tile by a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localDivideScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match {
        case i: Int => F.localDivideScalarInt(_: Tile, i)
        case d: Double => F.localDivideScalar(_: Tile, d)
      }

      udf(f).apply(tileCol).as(s"localDivideScalar($tileCol, $value)").as[Tile]
    }

    /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAlgebra(op: LocalTileBinaryOp, left: Column, right: Column):
    TypedColumn[Any, Tile] =
      withAlias(opName(op), left, right)(
        udf[Tile, Tile, Tile](op.apply).apply(left, right)
      ).as[Tile]

    /** Compute the normalized difference of two tile columns */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def normalizedDifference(left: Column, right: Column): TypedColumn[Any, Tile] =
      withAlias("normalizedDifference", left, right)(
        udf(F.normalizedDifference).apply(left, right)
      ).as[Tile]

    /** Constructor for constant tile column */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def makeConstantTile(value: Number, cols: Int, rows: Int, cellType: String): TypedColumn[Any, Tile] =
      udf(() => F.makeConstantTile(value, cols, rows, cellType)).apply().as(s"constant_$cellType").as[Tile]

    /** Alias for column of constant tiles of zero */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileZeros(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
      udf(() => F.tileZeros(cols, rows, cellType)).apply().as(s"zeros_$cellType").as[Tile]

    /** Alias for column of constant tiles of one */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileOnes(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
      udf(() => F.tileOnes(cols, rows, cellType)).apply().as(s"ones_$cellType").as[Tile]

    /** Where the mask tile equals the mask value, replace values in the source tile with NODATA */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def maskByValue(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
      withAlias("maskByValue", sourceTile, maskTile, maskValue)(
        udf(F.maskByValue).apply(sourceTile, maskTile, maskValue)
      ).as[Tile]

    /** Where the mask tile DOES NOT contain NODATA, replace values in the source tile with NODATA */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def inverseMask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
      withAlias("inverseMask", sourceTile, maskTile)(
        udf(F.inverseMask).apply(sourceTile, maskTile)
      ).as[Tile]

    /** Reproject a column of geometry from one CRS to another. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def reprojectGeometry(sourceGeom: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
      withAlias("reprojectGeometry", sourceGeom)(
        udf(F.reprojectGeometry(_: Geometry, srcCRS, dstCRS)).apply(sourceGeom)
      ).as[Geometry]

    /** Render Tile as ASCII string for debugging purposes. */
    @Experimental
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def renderAscii(col: Column): TypedColumn[Any, String] =
    withAlias("renderAscii", col)(
      udf[String, Tile](F.renderAscii).apply(col)
    ).as[String]

    /** Cellwise less than value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLess(left: Column, right: Column): TypedColumn[Any, Tile] =
      withAlias("localLess", left, right)(
        udf(F.localLess).apply(left, right)
      ).as[Tile]


    /** Cellwise less than value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLessScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match{
        case i: Int => F.localLessScalarInt(_: Tile, i)
        case d: Double => F.localLessScalar(_: Tile, d)
      }
      udf(f).apply(tileCol).as(s"localLessScalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise less than or equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLessEqual(left: Column, right: Column): TypedColumn[Any, Tile]  =
      withAlias("localLessEqual", left, right)(
        udf(F.localLess).apply(left, right)
      ).as[Tile]

    /** Cellwise less than or equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLessEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match{
        case i: Int => F.localLessEqualScalarInt(_: Tile, i)
        case d: Double => F.localLessEqualScalar(_: Tile, d)
      }
      udf(f).apply(tileCol).as(s"localLessEqualScalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise greater than value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreater(left: Column, right: Column): TypedColumn[Any, Tile] =
      withAlias("localGreater", left, right)(
        udf(F.localGreater).apply(left, right)
      ).as[Tile]


    /** Cellwise greater than value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreaterScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match{
        case i: Int => F.localGreaterScalarInt(_: Tile, i)
        case d: Double => F.localGreaterScalar(_: Tile, d)
      }
      udf(f).apply(tileCol).as(s"localGreaterScalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise greater than or equal to value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreaterEqual(left: Column, right: Column): TypedColumn[Any, Tile]  =
      withAlias("localGreaterEqual", left, right)(
        udf(F.localGreaterEqual).apply(left, right)
      ).as[Tile]

    /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreaterEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match{
        case i: Int => F.localGreaterEqualScalarInt(_: Tile, i)
        case d: Double => F.localGreaterEqualScalar(_: Tile, d)
      }
      udf(f).apply(tileCol).as(s"localGreaterEqualScalar($tileCol, $value)").as[Tile]
    }

    /** Cellwise equal to value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localEqual(left: Column, right: Column): TypedColumn[Any, Tile]  =
      withAlias("localEqual", left, right)(
        udf(F.localEqual).apply(left, right)
      ).as[Tile]

    /** Cellwise equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match{
        case i: Int => F.localEqualScalarInt(_: Tile, i)
        case d: Double => F.localEqualScalar(_: Tile, d)
      }
      udf(f).apply(tileCol).as(s"localEqualScalar($tileCol, $value)").as[Tile]
    }
    /** Cellwise inequality comparison between two tiles. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localUnequal(left: Column, right: Column): TypedColumn[Any, Tile]  =
      withAlias("localUnequal", left, right)(
        udf(F.localUnequal).apply(left, right)
      ).as[Tile]

    /** Cellwise inequality comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localUnequalScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
      val f = value match{
        case i: Int => F.localUnequalScalarInt(_: Tile, i)
        case d: Double => F.localUnequalScalar(_: Tile, d)
      }
      udf(f).apply(tileCol).as(s"localUnequalScalar($tileCol, $value)").as[Tile]
    }
  }

  def register(sqlContext: SQLContext): Unit = {

    /** Unary expression builder builder. */
    def ub[A, B](f: A => B)(a: Seq[A]): B = f(a.head)
    /** Binary expression builder builder. */
    def bb[A, B](f: (A, A) => B)(a: Seq[A]): B = f(a.head, a.last)

    // Expression-oriented functions have a different registration scheme
    // Currently have to register with the `builtin` registry due to Spark data hiding.
    val registry: FunctionRegistry = rf.registry(sqlContext)
    VersionShims.registerExpression(registry, "rf_explodeTiles", E.ExplodeTiles.apply(1.0, None, _))
    VersionShims.registerExpression(registry, "rf_cellType", ub(E.GetCellType.apply))
    VersionShims.registerExpression(registry, "rf_convertCellType", bb(E.SetCellType.apply))
    VersionShims.registerExpression(registry, "rf_tileDimensions", ub(E.GetDimensions.apply))
    VersionShims.registerExpression(registry, "rf_boundsGeometry", ub(E.BoundsToGeometry.apply))
    VersionShims.registerExpression(registry, "rf_localAdd", bb(BinaryRasterOp.Add))

    sqlContext.udf.register("rf_maskByValue", F.maskByValue)
    sqlContext.udf.register("rf_inverseMask", F.inverseMask)
    sqlContext.udf.register("rf_makeConstantTile", F.makeConstantTile)
    sqlContext.udf.register("rf_tileZeros", F.tileZeros)
    sqlContext.udf.register("rf_tileOnes", F.tileOnes)
    sqlContext.udf.register("rf_tileToArrayInt", F.tileToArray[Int])
    sqlContext.udf.register("rf_tileToArrayDouble", F.tileToArray[Double])
    sqlContext.udf.register("rf_aggHistogram", F.aggHistogram)
    sqlContext.udf.register("rf_aggStats", F.aggStats)
    sqlContext.udf.register("rf_tileMin", F.tileMin)
    sqlContext.udf.register("rf_tileMax", F.tileMax)
    sqlContext.udf.register("rf_tileMean", F.tileMean)
    sqlContext.udf.register("rf_tileSum", F.tileSum)
    sqlContext.udf.register("rf_tileHistogram", F.tileHistogram)
    sqlContext.udf.register("rf_tileStats", F.tileStats)
    sqlContext.udf.register("rf_dataCells", F.dataCells)
    sqlContext.udf.register("rf_noDataCells", F.noDataCells)
    sqlContext.udf.register("rf_isNoDataTile", F.isNoDataTile)
    sqlContext.udf.register("rf_localAggStats", F.localAggStats)
    sqlContext.udf.register("rf_localAggMax", F.localAggMax)
    sqlContext.udf.register("rf_localAggMin", F.localAggMin)
    sqlContext.udf.register("rf_localAggMean", F.localAggMean)
    sqlContext.udf.register("rf_localAggCount", F.localAggCount)
    sqlContext.udf.register("rf_localAddScalar", F.localAddScalar)
    sqlContext.udf.register("rf_localAddScalarInt", F.localAddScalarInt)
    sqlContext.udf.register("rf_localSubtract", F.localSubtract)
    sqlContext.udf.register("rf_localSubtractScalar", F.localSubtractScalar)
    sqlContext.udf.register("rf_localSubtractScalarInt", F.localSubtractScalarInt)
    sqlContext.udf.register("rf_localMultiply", F.localMultiply)
    sqlContext.udf.register("rf_localMultiplyScalar", F.localMultiplyScalar)
    sqlContext.udf.register("rf_localMultiplyScalarInt", F.localMultiplyScalarInt)
    sqlContext.udf.register("rf_localDivide", F.localDivide)
    sqlContext.udf.register("rf_localDivideScalar", F.localDivideScalar)
    sqlContext.udf.register("rf_localDivideScalarInt", F.localDivideScalarInt)
    sqlContext.udf.register("rf_normalizedDifference", F.normalizedDifference)
    sqlContext.udf.register("rf_cellTypes", F.cellTypes)
    sqlContext.udf.register("rf_renderAscii", F.renderAscii)
    sqlContext.udf.register("rf_lessScalar", F.localLessScalar)
    sqlContext.udf.register("rf_lessScalarInt", F.localLessScalarInt)
    sqlContext.udf.register("rf_lessEqual", F.localLessEqual)
    sqlContext.udf.register("rf_lessEqualScalar", F.localLessEqualScalar)
    sqlContext.udf.register("rf_lessEqualScalarInt", F.localLessEqualScalarInt)
    sqlContext.udf.register("rf_greater", F.localGreater)
    sqlContext.udf.register("rf_greaterScalar", F.localGreaterScalar)
    sqlContext.udf.register("rf_greaterScalarInt", F.localGreaterScalarInt)
    sqlContext.udf.register("rf_greaterEqual", F.localGreaterEqual)
    sqlContext.udf.register("rf_greaterEqualScalar", F.localGreaterEqualScalar)
    sqlContext.udf.register("rf_greaterEqualScalarInt", F.localGreaterEqualScalarInt)
    sqlContext.udf.register("rf_equal", F.localEqual)
    sqlContext.udf.register("rf_equalScalar", F.localEqualScalar)
    sqlContext.udf.register("rf_equalScalarInt", F.localEqualScalarInt)
    sqlContext.udf.register("rf_unequal", F.localUnequal)
    sqlContext.udf.register("rf_unequalScalar", F.localUnequalScalar)
    sqlContext.udf.register("rf_unequalScalarInt", F.localUnequalScalarInt)
    sqlContext.udf.register("rf_reprojectGeometry", F.reprojectGeometryCRSName)
  }
}
