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

package org.locationtech.rasterframes.functions
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.util.{opName, withTypedAlias}

/** Functions that operate on one or ore tiles and create a new tile on a cell-by-cell basis. */
trait LocalFunctions {
  import org.locationtech.rasterframes.encoders.StandardEncoders._

  /** Cellwise addition between two Tiles or Tile and scalar column. */
  def rf_local_add(left: Column, right: Column): Column = Add(left, right)

  /** Cellwise addition of a scalar value to a tile. */
  def rf_local_add[T: Numeric](tileCol: Column, value: T): Column = Add(tileCol, value)

  /** Cellwise subtraction between two Tiles. */
  def rf_local_subtract(left: Column, right: Column): Column = Subtract(left, right)

  /** Cellwise subtraction of a scalar value from a tile. */
  def rf_local_subtract[T: Numeric](tileCol: Column, value: T): Column = Subtract(tileCol, value)

  /** Cellwise multiplication between two Tiles. */
  def rf_local_multiply(left: Column, right: Column): Column = Multiply(left, right)

  /** Cellwise multiplication of a tile by a scalar value. */
  def rf_local_multiply[T: Numeric](tileCol: Column, value: T): Column = Multiply(tileCol, value)

  /** Cellwise division between two Tiles. */
  def rf_local_divide(left: Column, right: Column): Column = Divide(left, right)

  /** Cellwise division of a tile by a scalar value. */
  def rf_local_divide[T: Numeric](tileCol: Column, value: T): Column = Divide(tileCol, value)

  /** Cellwise minimum between Tiles. */
  def rf_local_min(left: Column, right: Column): Column = Min(left, right)

  /** Cellwise minimum between Tiles. */
  def rf_local_min[T: Numeric](left: Column, right: T): Column = Min(left, right)

  /** Cellwise maximum between Tiles. */
  def rf_local_max(left: Column, right: Column): Column = Max(left, right)

  /** Cellwise maximum between Tiles. */
  def rf_local_max[T: Numeric](left: Column, right: T): Column = Max(left, right)

  /** Return the tile with its values clipped to a range defined by min and max. */
  def rf_local_clip(tile: Column, min: Column, max: Column) = Clip(tile, min, max)

  /** Return the tile with its values clipped to a range defined by min and max. */
  def rf_local_clip[T: Numeric](tile: Column, min: T, max: Column) = Clip(tile, min, max)

  /** Return the tile with its values clipped to a range defined by min and max. */
  def rf_local_clip[T: Numeric](tile: Column, min: Column, max: T) = Clip(tile, min, max)

  /** Return the tile with its values clipped to a range defined by min and max. */
  def rf_local_clip[T: Numeric](tile: Column, min: T, max: T) = Clip(tile, min, max)

  /** Return a tile with cell values chosen from `x` or `y` depending on `condition`.
      Operates cell-wise in a similar fashion to Spark SQL `when` and `otherwise`. */
  def rf_where(condition: Column, x: Column, y: Column): Column = Where(condition, x, y)

  /** Standardize cell values such that the mean is zero and the standard deviation is one.
    * The `mean` and `stddev` are applied to all tiles in the column.
    */
  def rf_standardize(tile: Column, mean: Column, stddev: Column): Column = Standardize(tile, mean, stddev)

  /** Standardize cell values such that the mean is zero and the standard deviation is one.
    * The `mean` and `stddev` are applied to all tiles in the column.
    */
  def rf_standardize(tile: Column, mean: Double, stddev: Double): Column = Standardize(tile, mean, stddev)

  /** Standardize cell values such that the mean is zero and the standard deviation is one.
    * Each tile will be standardized according to the statistics of its cell values; this can result in inconsistent values across rows in a tile column. */
  def rf_standardize(tile: Column): Column = Standardize(tile)


  /** Rescale cell values such that the minimum is zero and the maximum is one. Other values will be linearly interpolated into the range.
    * Cells with the tile-wise minimum value will become the zero value and those at the tile-wise maximum value will become 1.
    *  This can result in inconsistent values across rows in a tile column.
    */
  def rf_rescale(tile: Column): Column = Rescale(tile)

  /** Rescale cell values such that the minimum is zero and the maximum is one. Other values will be linearly interpolated into the range.
    * The `min` parameter will become the zero value and the `max` parameter will become 1.
    * Values outside the range will be clipped to 0 or 1.
    */
  def rf_rescale(tile: Column, min: Column, max: Column): Column = Rescale(tile, min, max)

  /** Rescale cell values such that the minimum is zero and the maximum is one. Other values will be linearly interpolated into the range.
    * The `min` parameter will become the zero value and the `max` parameter will become 1.
    * Values outside the range will be clipped to 0 or 1.
    */
  def rf_rescale(tile: Column, min: Double, max: Double): Column = Rescale(tile, min, max)

  /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
  def rf_local_algebra(op: LocalTileBinaryOp, left: Column, right: Column): TypedColumn[Any, Tile] =
    withTypedAlias(opName(op), left, right)(udf[Tile, Tile, Tile](op.apply).apply(left, right))

  /** Compute the normalized difference of two tile columns */
  def rf_normalized_difference(left: Column, right: Column) =
    NormalizedDifference(left, right)

  /** Where the rf_mask tile contains NODATA, replace values in the source tile with NODATA */
  def rf_mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] = rf_mask(sourceTile, maskTile, false)

  /** Where the rf_mask tile contains NODATA, replace values in the source tile with NODATA */
  def rf_mask(sourceTile: Column, maskTile: Column, inverse: Boolean = false): TypedColumn[Any, Tile] =
    if (!inverse) Mask.MaskByDefined(sourceTile, maskTile)
    else Mask.InverseMaskByDefined(sourceTile, maskTile)

  /** Where the `maskTile` equals `maskValue`, replace values in the source tile with `NoData` */
  def rf_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Column, inverse: Boolean = false): TypedColumn[Any, Tile] =
    if (!inverse) Mask.MaskByValue(sourceTile, maskTile, maskValue)
    else Mask.InverseMaskByValue(sourceTile, maskTile, maskValue)

  /** Where the `maskTile` equals `maskValue`, replace values in the source tile with `NoData` */
  def rf_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Int, inverse: Boolean): TypedColumn[Any, Tile] =
    rf_mask_by_value(sourceTile, maskTile, lit(maskValue), inverse)

  /** Where the `maskTile` equals `maskValue`, replace values in the source tile with `NoData` */
  def rf_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Int): TypedColumn[Any, Tile] =
    rf_mask_by_value(sourceTile, maskTile, maskValue, false)

  /** Generate a tile with the values from `data_tile`, but where cells in the `mask_tile` are in the `mask_values`
       list, replace the value with NODATA. */
  def rf_mask_by_values(sourceTile: Column, maskTile: Column, maskValues: Column): TypedColumn[Any, Tile] =
    Mask.MaskByValues(sourceTile, maskTile, maskValues)

  /** Generate a tile with the values from `data_tile`, but where cells in the `mask_tile` are in the `mask_values`
       list, replace the value with NODATA. */
  def rf_mask_by_values(sourceTile: Column, maskTile: Column, maskValues: Int*): TypedColumn[Any, Tile] = {
    import org.apache.spark.sql.functions.array
    val valuesCol: Column = array(maskValues.map(lit).toSeq: _*)
    rf_mask_by_values(sourceTile, maskTile, valuesCol)
  }

  /** Where the `maskTile` does **not** contain `NoData`, replace values in the source tile with `NoData` */
  def rf_inverse_mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    Mask.InverseMaskByDefined(sourceTile, maskTile)

  /** Where the `maskTile` does **not** equal `maskValue`, replace values in the source tile with `NoData` */
  def rf_inverse_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    Mask.InverseMaskByValue(sourceTile, maskTile, maskValue)

  /** Where the `maskTile` does **not** equal `maskValue`, replace values in the source tile with `NoData` */
  def rf_inverse_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Int): TypedColumn[Any, Tile] =
    Mask.InverseMaskByValue(sourceTile, maskTile, lit(maskValue))

  /** Applies a mask using bit values in the `mask_tile`. Working from the right, extract the bit at `bitPosition` from the `maskTile`. In all locations where these are equal to the `valueToMask`, the returned tile is set to NoData, else the original `dataTile` cell value. */
  def rf_mask_by_bit(dataTile: Column, maskTile: Column, bitPosition: Int, valueToMask: Boolean): TypedColumn[Any, Tile] =
    rf_mask_by_bit(dataTile, maskTile, lit(bitPosition), lit(if (valueToMask) 1 else 0))

  /** Applies a mask using bit values in the `mask_tile`. Working from the right, extract the bit at `bitPosition` from the `maskTile`. In all locations where these are equal to the `valueToMask`, the returned tile is set to NoData, else the original `dataTile` cell value. */
  def rf_mask_by_bit(dataTile: Column, maskTile: Column, bitPosition: Column, valueToMask: Column): TypedColumn[Any, Tile] = {
    import org.apache.spark.sql.functions.array
    rf_mask_by_bits(dataTile, maskTile, bitPosition, lit(1), array(valueToMask))
  }

  /** Applies a mask from blacklisted bit values in the `mask_tile`. Working from the right, the bits from `start_bit` to `start_bit + num_bits` are @ref:[extracted](reference.md#rf_local_extract_bits) from cell values of the `mask_tile`. In all locations where these are in the `mask_values`, the returned tile is set to NoData; otherwise the original `tile` cell value is returned. */
  def rf_mask_by_bits(
    dataTile: Column,
    maskTile: Column,
    startBit: Column,
    numBits: Column,
    valuesToMask: Column): TypedColumn[Any, Tile] = {
    val bitMask = rf_local_extract_bits(maskTile, startBit, numBits)
    rf_mask_by_values(dataTile, bitMask, valuesToMask)
  }

  /** Applies a mask from blacklisted bit values in the `mask_tile`. Working from the right, the bits from `start_bit` to `start_bit + num_bits` are @ref:[extracted](reference.md#rf_local_extract_bits) from cell values of the `mask_tile`. In all locations where these are in the `mask_values`, the returned tile is set to NoData; otherwise the original `tile` cell value is returned. */
  def rf_mask_by_bits(dataTile: Column, maskTile: Column, startBit: Int, numBits: Int, valuesToMask: Int*): TypedColumn[Any, Tile] = {
    import org.apache.spark.sql.functions.array
    val values = array(valuesToMask.map(lit): _*)
    rf_mask_by_bits(dataTile, maskTile, lit(startBit), lit(numBits), values)
  }

  /** Extract value from specified bits of the cells' underlying binary data.
   * `startBit` is the first bit to consider, working from the right. It is zero indexed.
   * `numBits` is the number of bits to take moving further to the left. */
  def rf_local_extract_bits(tile: Column, startBit: Column, numBits: Column): Column =
    ExtractBits(tile, startBit, numBits)

  /** Extract value from specified bits of the cells' underlying binary data.
   * `bitPosition` is bit to consider, working from the right. It is zero indexed. */
  def rf_local_extract_bits(tile: Column, bitPosition: Column): Column =
    rf_local_extract_bits(tile, bitPosition, lit(1))

  /** Extract value from specified bits of the cells' underlying binary data.
   * `startBit` is the first bit to consider, working from the right. It is zero indexed.
   * `numBits` is the number of bits to take, moving further to the left. */
  def rf_local_extract_bits(tile: Column, startBit: Int, numBits: Int): Column =
    rf_local_extract_bits(tile, lit(startBit), lit(numBits))

  /** Extract value from specified bits of the cells' underlying binary data.
   * `bitPosition` is bit to consider, working from the right. It is zero indexed. */
  def rf_local_extract_bits(tile: Column, bitPosition: Int): Column =
    rf_local_extract_bits(tile, lit(bitPosition))

  /** Cellwise less than value comparison between two tiles. */
  def rf_local_less(left: Column, right: Column): Column = Less(left, right)

  /** Cellwise less than value comparison between a tile and a scalar. */
  def rf_local_less[T: Numeric](tileCol: Column, value: T): Column = Less(tileCol, value)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def rf_local_less_equal(left: Column, right: Column): Column = LessEqual(left, right)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def rf_local_less_equal[T: Numeric](tileCol: Column, value: T): Column = LessEqual(tileCol, value)

  /** Cellwise greater than value comparison between two tiles. */
  def rf_local_greater(left: Column, right: Column): Column = Greater(left, right)

  /** Cellwise greater than value comparison between a tile and a scalar. */
  def rf_local_greater[T: Numeric](tileCol: Column, value: T): Column = Greater(tileCol, value)

  /** Cellwise greater than or equal to value comparison between two tiles. */
  def rf_local_greater_equal(left: Column, right: Column): Column = GreaterEqual(left, right)

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  def rf_local_greater_equal[T: Numeric](tileCol: Column, value: T): Column = GreaterEqual(tileCol, value)

  /** Cellwise equal to value comparison between two tiles. */
  def rf_local_equal(left: Column, right: Column): Column = Equal(left, right)

  /** Cellwise equal to value comparison between a tile and a scalar. */
  def rf_local_equal[T: Numeric](tileCol: Column, value: T): Column = Equal(tileCol, value)

  /** Cellwise inequality comparison between two tiles. */
  def rf_local_unequal(left: Column, right: Column): Column = Unequal(left, right)

  /** Cellwise inequality comparison between a tile and a scalar. */
  def rf_local_unequal[T: Numeric](tileCol: Column, value: T): Column = Unequal(tileCol, value)

  /** Test if each cell value is in provided array */
  def rf_local_is_in(tileCol: Column, arrayCol: Column) = IsIn(tileCol, arrayCol)

  /** Test if each cell value is in provided array */
  def rf_local_is_in(tileCol: Column, array: Array[Int]) = IsIn(tileCol, array)

  /** Return a tile with ones where the input is NoData, otherwise zero */
  def rf_local_no_data(tileCol: Column): Column = Undefined(tileCol)

  /** Return a tile with zeros where the input is NoData, otherwise one*/
  def rf_local_data(tileCol: Column): Column = Defined(tileCol)

  /** Round cell values to nearest integer without chaning cell type. */
  def rf_round(tileCol: Column): Column = Round(tileCol)

  /** Compute the absolute value of each cell. */
  def rf_abs(tileCol: Column): Column = Abs(tileCol)

  /** Take natural logarithm of cell values. */
  def rf_log(tileCol: Column): Column = Log(tileCol)

  /** Take base 10 logarithm of cell values. */
  def rf_log10(tileCol: Column): Column = Log10(tileCol)

  /** Take base 2 logarithm of cell values. */
  def rf_log2(tileCol: Column): Column = Log2(tileCol)

  /** Natural logarithm of one plus cell values. */
  def rf_log1p(tileCol: Column): Column = Log1p(tileCol)

  /** Exponential of cell values */
  def rf_exp(tileCol: Column): Column = Exp(tileCol)

  /** Ten to the power of cell values */
  def rf_exp10(tileCol: Column): Column = Exp10(tileCol)

  /** Two to the power of cell values */
  def rf_exp2(tileCol: Column): Column = Exp2(tileCol)

  /** Exponential of cell values, less one*/
  def rf_expm1(tileCol: Column): Column = ExpM1(tileCol)

  /** Return the incoming tile untouched. */
  def rf_identity(tileCol: Column): Column = Identity(tileCol)
}

object LocalFunctions extends LocalFunctions
