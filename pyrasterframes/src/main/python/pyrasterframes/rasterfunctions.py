#
# This software is licensed under the Apache 2 license, quoted below.
#
# Copyright 2019 Astraea, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# [http://www.apache.org/licenses/LICENSE-2.0]
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# SPDX-License-Identifier: Apache-2.0
#

"""
This module creates explicit Python functions that map back to the existing Scala
implementations. Most functions are standard Column functions, but those with unique
signatures are handled here as well.
"""
from __future__ import absolute_import
from pyspark.sql.column import Column, _to_java_column
from .context import RFContext
from .rf_types import CellType

THIS_MODULE = 'pyrasterframes'


def _context_call(name, *args):
    f = RFContext.active().lookup(name)
    return f(*args)


def _parse_cell_type(cell_type_str):
    """ Convert the string cell type to the expected CellType object."""
    return _context_call('_parse_cell_type', cell_type_str)


def rf_cell_types():
    """Return a list of standard cell types"""
    return [CellType(str(ct)) for ct in _context_call('rf_cell_types')]


def rf_assemble_tile(col_index, row_index, cell_data_col, num_cols, num_rows, cell_type_str):
    """Create a Tile from  a column of cell data with location indices"""
    jfcn = RFContext.active().lookup('rf_assemble_tile')
    return Column(
        jfcn(_to_java_column(col_index), _to_java_column(row_index), _to_java_column(cell_data_col), num_cols, num_rows,
             _parse_cell_type(cell_type_str)))


def rf_array_to_tile(array_col, num_cols, num_rows):
    """Convert array in `array_col` into a Tile of dimensions `num_cols` and `num_rows'"""
    jfcn = RFContext.active().lookup('rf_array_to_tile')
    return Column(jfcn(_to_java_column(array_col), num_cols, num_rows))


def rf_convert_cell_type(tile_col, cell_type):
    """Convert the numeric type of the Tiles in `tileCol`"""
    jfcn = RFContext.active().lookup('rf_convert_cell_type')
    return Column(jfcn(_to_java_column(tile_col), _parse_cell_type(cell_type)))


def rf_make_constant_tile(value, cols, rows, cell_type):
    """Constructor for constant tile column"""
    jfcn = RFContext.active().lookup('rf_make_constant_tile')
    return Column(jfcn(value, cols, rows, cell_type))


def rf_make_zeros_tile(cols, rows, cell_type='float64'):
    """Create column of constant tiles of zero"""
    jfcn = RFContext.active().lookup('rf_make_zeros_tile')
    return Column(jfcn(cols, rows, cell_type))


def rf_make_ones_tile(cols, rows, cell_type='float64'):
    """Create column of constant tiles of one"""
    jfcn = RFContext.active().lookup('rf_make_ones_tile')
    return Column(jfcn(cols, rows, cell_type))


def rf_rasterize(geometry_col, bounds_col, value_col, num_cols, num_rows):
    """Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value."""
    jfcn = RFContext.active().lookup('rf_rasterize')
    return Column(jfcn(_to_java_column(geometry_col), _to_java_column(bounds_col), _to_java_column(value_col), num_cols,
                       num_rows))


def st_reproject(geometry_col, src_crs_name, dst_crs_name):
    """Reproject a column of geometry given the CRS names of the source and destination.
    Currently supported registries are EPSG, ESRI, WORLD, NAD83, & NAD27.
    An example of a valid CRS name is EPSG:3005."""
    jfcn = RFContext.active().lookup('st_reproject')
    return Column(jfcn(_to_java_column(geometry_col), src_crs_name, dst_crs_name))


def rf_explode_tiles(*args):
    """Create a row for each cell in Tile."""
    jfcn = RFContext.active().lookup('rf_explode_tiles')
    jcols = [_to_java_column(arg) for arg in args]
    return Column(jfcn(RFContext.active().list_to_seq(jcols)))


def rf_explode_tiles_sample(sample_frac, seed, *tile_cols):
    """Create a row for a sample of cells in Tile columns."""
    jfcn = RFContext.active().lookup('rf_explode_tiles_sample')
    jcols = [_to_java_column(arg) for arg in tile_cols]
    return Column(jfcn(sample_frac, seed, RFContext.active().list_to_seq(jcols)))


def rf_mask_by_value(data_tile, mask_tile, mask_value):
    """Generate a tile with the values from the data tile, but where cells in the masking tile contain the masking
    value, replace the data value with NODATA. """
    jfcn = RFContext.active().lookup('rf_mask_by_value')
    return Column(jfcn(_to_java_column(data_tile), _to_java_column(mask_tile), _to_java_column(mask_value)))


def rf_inverse_mask_by_value(data_tile, mask_tile, mask_value):
    """Generate a tile with the values from the data tile, but where cells in the masking tile do not contain the
    masking value, replace the data value with NODATA. """
    jfcn = RFContext.active().lookup('rf_inverse_mask_by_value')
    return Column(jfcn(_to_java_column(data_tile), _to_java_column(mask_tile), _to_java_column(mask_value)))


def _apply_scalar_to_tile(name, tile_col, scalar):
    jfcn = RFContext.active().lookup(name)
    return Column(jfcn(_to_java_column(tile_col), scalar))


def rf_with_no_data(tile_col, scalar):
    """Assign a `NoData` value to the Tiles in the given Column."""
    return _apply_scalar_to_tile('rf_with_no_data', tile_col, scalar)


def rf_local_add_double(tile_col, scalar):
    """Add a floating point scalar to a Tile"""
    return _apply_scalar_to_tile('rf_local_add_double', tile_col, scalar)


def rf_local_add_int(tile_col, scalar):
    """Add an integral scalar to a Tile"""
    return _apply_scalar_to_tile('rf_local_add_int', tile_col, scalar)


def rf_local_subtract_double(tile_col, scalar):
    """Subtract a floating point scalar from a Tile"""
    return _apply_scalar_to_tile('rf_local_subtract_double', tile_col, scalar)


def rf_local_subtract_int(tile_col, scalar):
    """Subtract an integral scalar from a Tile"""
    return _apply_scalar_to_tile('rf_local_subtract_int', tile_col, scalar)


def rf_local_multiply_double(tile_col, scalar):
    """Multiply a Tile by a float point scalar"""
    return _apply_scalar_to_tile('rf_local_multiply_double', tile_col, scalar)


def rf_local_multiply_int(tile_col, scalar):
    """Multiply a Tile by an integral scalar"""
    return _apply_scalar_to_tile('rf_local_multiply_int', tile_col, scalar)


def rf_local_divide_double(tile_col, scalar):
    """Divide a Tile by a floating point scalar"""
    return _apply_scalar_to_tile('rf_local_divide_double', tile_col, scalar)


def rf_local_divide_int(tile_col, scalar):
    """Divide a Tile by an integral scalar"""
    return _apply_scalar_to_tile('rf_local_divide_int', tile_col, scalar)


def rf_local_less_double(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0"""
    return _apply_scalar_to_tile('foo', tile_col, scalar)


def rf_local_less_int(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_less_double', tile_col, scalar)


def rf_local_less_equal_double(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_less_equal_double', tile_col, scalar)


def rf_local_less_equal_int(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_less_equal_int', tile_col, scalar)


def rf_local_greater_double(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_greater_double', tile_col, scalar)


def rf_local_greater_int(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_greater_int', tile_col, scalar)


def rf_local_greater_equal_double(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_greater_equal_double', tile_col, scalar)


def rf_local_greater_equal_int(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_greater_equal_int', tile_col, scalar)


def rf_local_equal_double(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_equal_double', tile_col, scalar)


def rf_local_equal_int(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_equal_int', tile_col, scalar)


def rf_local_unequal_double(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_unequal_double', tile_col, scalar)


def rf_local_unequal_int(tile_col, scalar):
    """Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0"""
    return _apply_scalar_to_tile('rf_local_unequal_int', tile_col, scalar)


def _apply_column_function(name, *args):
    jfcn = RFContext.active().lookup(name)
    jcols = [_to_java_column(arg) for arg in args]
    return Column(jfcn(*jcols))


def rf_dimensions(*args):
    """Query the number of (cols, rows) in a Tile."""
    return _apply_column_function('rf_dimensions', *args)


def rf_tile_to_array_int(*args):
    """Flattens Tile into an array of integers."""
    return _apply_column_function('rf_tile_to_array_int', *args)


def rf_tile_to_array_double(*args):
    """Flattens Tile into an array of doubles."""
    return _apply_column_function('rf_tile_to_array_double', *args)


def rf_cell_type(*args):
    """Extract the Tile's cell type"""
    return _apply_column_function('rf_cell_type', *args)


def rf_is_no_data_tile(*args):
    """Report if the Tile is entirely NODDATA cells"""
    return _apply_column_function('rf_is_no_data_tile', *args)


def rf_exists(*args):
    """Returns true if any cells in the tile are true (non-zero and not NoData)"""
    return _apply_column_function('rf_exists', *args)


def rf_for_all(*args):
    """Returns true if all cells in the tile are true (non-zero and not NoData)."""
    return _apply_column_function('rf_for_all', *args)


def rf_agg_approx_histogram(*args):
    """Compute the full column aggregate floating point histogram"""
    return _apply_column_function('rf_agg_approx_histogram', *args)


def rf_agg_stats(*args):
    """Compute the full column aggregate floating point statistics"""
    return _apply_column_function('rf_agg_stats', *args)


def rf_agg_mean(*args):
    """Computes the column aggregate mean"""
    return _apply_column_function('rf_agg_mean', *args)


def rf_agg_data_cells(*args):
    """Computes the number of non-NoData cells in a column"""
    return _apply_column_function('rf_agg_data_cells', *args)


def rf_agg_no_data_cells(*args):
    """Computes the number of NoData cells in a column"""
    return _apply_column_function('rf_agg_no_data_cells', *args)


def rf_tile_histogram(*args):
    """Compute the Tile-wise histogram"""
    return _apply_column_function('rf_tile_histogram', *args)


def rf_tile_mean(*args):
    """Compute the Tile-wise mean"""
    return _apply_column_function('rf_tile_mean', *args)


def rf_tile_sum(*args):
    """Compute the Tile-wise sum"""
    return _apply_column_function('rf_tile_sum', *args)


def rf_tile_min(*args):
    """Compute the Tile-wise minimum"""
    return _apply_column_function('rf_tile_min', *args)


def rf_tile_max(*args):
    """Compute the Tile-wise maximum"""
    return _apply_column_function('rf_tile_max', *args)


def rf_tile_stats(*args):
    """Compute the Tile-wise floating point statistics"""
    return _apply_column_function('rf_tile_stats', *args)


def rf_render_ascii(*args):
    """Render ASCII art of tile"""
    return _apply_column_function('rf_render_ascii', *args)


def rf_render_matrix(*args):
    """Render Tile cell values as numeric values, for debugging purposes"""
    return _apply_column_function('rf_render_matrix', *args)


def rf_no_data_cells(*args):
    """Count of NODATA cells"""
    return _apply_column_function('rf_no_data_cells', *args)


def rf_data_cells(*args):
    """Count of cells with valid data"""
    return _apply_column_function('rf_data_cells', *args)


def rf_local_add(*args):
    """Add two Tiles"""
    return _apply_column_function('rf_local_add', *args)


def rf_local_subtract(*args):
    """Subtract two Tiles"""
    return _apply_column_function('rf_local_subtract', *args)


def rf_local_multiply(*args):
    """Multiply two Tiles"""
    return _apply_column_function('rf_local_multiply', *args)


def rf_local_divide(*args):
    """Divide two Tiles"""
    return _apply_column_function('rf_local_divide', *args)


def rf_normalized_difference(*args):
    """Compute the normalized difference of two tiles"""
    return _apply_column_function('rf_normalized_difference', *args)


def rf_agg_local_max(*args):
    """Compute the cell-wise/local max operation between Tiles in a column."""
    return _apply_column_function('rf_agg_local_max', *args)


def rf_agg_local_min(*args):
    """Compute the cellwise/local min operation between Tiles in a column."""
    return _apply_column_function('rf_agg_local_min', *args)


def rf_agg_local_mean(*args):
    """Compute the cellwise/local mean operation between Tiles in a column."""
    return _apply_column_function('rf_agg_local_mean', *args)


def rf_agg_local_data_cells(*args):
    """Compute the cellwise/local count of non-NoData cells for all Tiles in a column."""
    return _apply_column_function('rf_agg_local_data_cells', *args)


def rf_agg_local_no_data_cells(*args):
    """Compute the cellwise/local count of NoData cells for all Tiles in a column."""
    return _apply_column_function('rf_agg_local_no_data_cells', *args)


def rf_agg_local_stats(*args):
    """Compute cell-local aggregate descriptive statistics for a column of Tiles."""
    return _apply_column_function('rf_agg_local_stats', *args)


def rf_mask(*args):
    """Where the rf_mask (second) tile contains NODATA, replace values in the source (first) tile with NODATA."""
    return _apply_column_function('rf_mask', *args)


def rf_inverse_mask(*args):
    """Where the rf_mask (second) tile DOES NOT contain NODATA, replace values in the source (first) tile with NODATA."""
    return _apply_column_function('rf_inverse_mask', *args)


def rf_local_less(*args):
    """Cellwise less than comparison between two tiles"""
    return _apply_column_function('rf_local_less', *args)


def rf_local_less_equal(*args):
    """Cellwise less than or equal to comparison between two tiles"""
    return _apply_column_function('rf_local_less_equal', *args)


def rf_local_greater(*args):
    """Cellwise greater than comparison between two tiles"""
    return _apply_column_function('rf_local_greater', *args)


def rf_local_greater_equal(*args):
    """Cellwise greater than or equal to comparison between two tiles"""
    return _apply_column_function('rf_local_greater_equal', *args)


def rf_local_equal(*args):
    """Cellwise equality comparison between two tiles"""
    return _apply_column_function('rf_local_equal', *args)


def rf_local_unequal(*args):
    """Cellwise inequality comparison between two tiles"""
    return _apply_column_function('rf_local_unequal', *args)


def rf_round(*args):
    """Round cell values to the nearest integer without changing the cell type"""
    return _apply_column_function('rf_round', *args)


def rf_abs(*args):
    """Compute the absolute value of each cell"""
    return _apply_column_function('rf_abs', *args)


def rf_log(*args):
    """Performs cell-wise natural logarithm"""
    return _apply_column_function('rf_log', *args)


def rf_log10(*args):
    """Performs cell-wise logartithm with base 10"""
    return _apply_column_function('rf_log10', *args)


def rf_log2(*args):
    """Performs cell-wise logartithm with base 2"""
    return _apply_column_function('rf_log2', *args)


def rf_log1p(*args):
    """Performs natural logarithm of cell values plus one"""
    return _apply_column_function('rf_log1p', *args)


def rf_exp(*args):
    """Performs cell-wise exponential"""
    return _apply_column_function('rf_exp', *args)


def rf_exp2(*args):
    """Compute 2 to the power of cell values"""
    return _apply_column_function('rf_exp2', *args)


def rf_exp10(*args):
    """Compute 10 to the power of cell values"""
    return _apply_column_function('rf_exp10', *args)


def rf_expm1(*args):
    """Performs cell-wise exponential, then subtract one"""
    return _apply_column_function('rf_expm1', *args)


def rf_identity(*args):
    """Pass tile through unchanged"""
    return _apply_column_function('rf_identity', *args)


def rf_resample(*args):
    """Resample tile to different size based on scalar factor or tile whose dimension to match"""
    return _apply_column_function('rf_resample', *args)


def rf_crs(*args):
    """Get the CRS of a RasterSource or ProjectedRasterTile"""
    return _apply_column_function('rf_crs', *args)


def st_extent(*args):
    """Compute the extent/bbox of a Geometry (a tile with embedded extent and CRS)"""
    return _apply_column_function('st_extent', *args)


def rf_extent(*args):
    """Get the extent of a RasterSource or ProjectedRasterTile (a tile with embedded extent and CRS)"""
    return _apply_column_function('rf_extent', *args)


def rf_tile(*args):
    """Extracts the Tile component of a ProjectedRasterTile (or Tile)."""
    return _apply_column_function('rf_tile', *args)


def st_geometry(*args):
    """Convert the given extent/bbox to a polygon"""
    return _apply_column_function('st_geometry', *args)


def rf_geometry(*args):
    """Get the extent of a RasterSource or ProjectdRasterTile as a Geometry"""
    return _apply_column_function('rf_geometry', *args)


def st_geomFromGeoHash(*args):
    """"""
    return _apply_column_function('st_geomFromGeoHash', *args)


def st_geomFromWKT(*args):
    """"""
    return _apply_column_function('st_geomFromWKT', *args)


def st_geomFromWKB(*args):
    """"""
    return _apply_column_function('st_geomFromWKB', *args)


def st_lineFromText(*args):
    """"""
    return _apply_column_function('st_lineFromText', *args)


def st_makeBox2D(*args):
    """"""
    return _apply_column_function('st_makeBox2D', *args)


def st_makeBBox(*args):
    """"""
    return _apply_column_function('st_makeBBox', *args)


def st_makePolygon(*args):
    """"""
    return _apply_column_function('st_makePolygon', *args)


def st_makePoint(*args):
    """"""
    return _apply_column_function('st_makePoint', *args)


def st_makeLine(*args):
    """"""
    return _apply_column_function('st_makeLine', *args)


def st_makePointM(*args):
    """"""
    return _apply_column_function('st_makePointM', *args)


def st_mLineFromText(*args):
    """"""
    return _apply_column_function('st_mLineFromText', *args)


def st_mPointFromText(*args):
    """"""
    return _apply_column_function('st_mPointFromText', *args)


def st_mPolyFromText(*args):
    """"""
    return _apply_column_function('st_mPolyFromText', *args)


def st_point(*args):
    """"""
    return _apply_column_function('st_point', *args)


def st_pointFromGeoHash(*args):
    """"""
    return _apply_column_function('st_pointFromGeoHash', *args)


def st_pointFromText(*args):
    """"""
    return _apply_column_function('st_pointFromText', *args)


def st_pointFromWKB(*args):
    """"""
    return _apply_column_function('st_pointFromWKB', *args)


def st_polygon(*args):
    """"""
    return _apply_column_function('st_polygon', *args)


def st_polygonFromText(*args):
    """"""
    return _apply_column_function('st_polygonFromText', *args)


def st_castToPoint(*args):
    """"""
    return _apply_column_function('st_castToPoint', *args)


def st_castToPolygon(*args):
    """"""
    return _apply_column_function('st_castToPolygon', *args)


def st_castToLineString(*args):
    """"""
    return _apply_column_function('st_castToLineString', *args)


def st_byteArray(*args):
    """"""
    return _apply_column_function('st_byteArray', *args)


def st_boundary(*args):
    """"""
    return _apply_column_function('st_boundary', *args)


def st_coordDim(*args):
    """"""
    return _apply_column_function('st_coordDim', *args)


def st_dimension(*args):
    """"""
    return _apply_column_function('st_dimension', *args)


def st_envelope(*args):
    """"""
    return _apply_column_function('st_envelope', *args)


def st_exteriorRing(*args):
    """"""
    return _apply_column_function('st_exteriorRing', *args)


def st_geometryN(*args):
    """"""
    return _apply_column_function('st_geometryN', *args)


def st_geometryType(*args):
    """"""
    return _apply_column_function('st_geometryType', *args)


def st_interiorRingN(*args):
    """"""
    return _apply_column_function('st_interiorRingN', *args)


def st_isClosed(*args):
    """"""
    return _apply_column_function('st_isClosed', *args)


def st_isCollection(*args):
    """"""
    return _apply_column_function('st_isCollection', *args)


def st_isEmpty(*args):
    """"""
    return _apply_column_function('st_isEmpty', *args)


def st_isRing(*args):
    """"""
    return _apply_column_function('st_isRing', *args)


def st_isSimple(*args):
    """"""
    return _apply_column_function('st_isSimple', *args)


def st_isValid(*args):
    """"""
    return _apply_column_function('st_isValid', *args)


def st_numGeometries(*args):
    """"""
    return _apply_column_function('st_numGeometries', *args)


def st_numPoints(*args):
    """"""
    return _apply_column_function('st_numPoints', *args)


def st_pointN(*args):
    """"""
    return _apply_column_function('st_pointN', *args)


def st_x(*args):
    """"""
    return _apply_column_function('st_x', *args)


def st_y(*args):
    """"""
    return _apply_column_function('st_y', *args)


def st_asBinary(*args):
    """"""
    return _apply_column_function('st_asBinary', *args)


def st_asGeoJSON(*args):
    """"""
    return _apply_column_function('st_asGeoJSON', *args)


def st_asLatLonText(*args):
    """"""
    return _apply_column_function('st_asLatLonText', *args)


def st_asText(*args):
    """"""
    return _apply_column_function('st_asText', *args)


def st_geoHash(*args):
    """"""
    return _apply_column_function('st_geoHash', *args)


def st_bufferPoint(*args):
    """"""
    return _apply_column_function('st_bufferPoint', *args)


def st_antimeridianSafeGeom(*args):
    """"""
    return _apply_column_function('st_antimeridianSafeGeom', *args)


def st_translate(*args):
    """"""
    return _apply_column_function('st_translate', *args)


def st_contains(*args):
    """"""
    return _apply_column_function('st_contains', *args)


def st_covers(*args):
    """"""
    return _apply_column_function('st_covers', *args)


def st_crosses(*args):
    """"""
    return _apply_column_function('st_crosses', *args)


def st_disjoint(*args):
    """"""
    return _apply_column_function('st_disjoint', *args)


def st_equals(*args):
    """"""
    return _apply_column_function('st_equals', *args)


def st_intersects(*args):
    """"""
    return _apply_column_function('st_intersects', *args)


def st_overlaps(*args):
    """"""
    return _apply_column_function('st_overlaps', *args)


def st_touches(*args):
    """"""
    return _apply_column_function('st_touches', *args)


def st_within(*args):
    """"""
    return _apply_column_function('st_within', *args)


def st_relate(*args):
    """"""
    return _apply_column_function('st_relate', *args)


def st_relateBool(*args):
    """"""
    return _apply_column_function('st_relateBool', *args)


def st_area(*args):
    """"""
    return _apply_column_function('st_area', *args)


def st_closestPoint(*args):
    """"""
    return _apply_column_function('st_closestPoint', *args)


def st_centroid(*args):
    """"""
    return _apply_column_function('st_centroid', *args)


def st_distance(*args):
    """"""
    return _apply_column_function('st_distance', *args)


def st_distanceSphere(*args):
    """"""
    return _apply_column_function('st_distanceSphere', *args)


def st_length(*args):
    """"""
    return _apply_column_function('st_length', *args)


def st_aggregateDistanceSphere(*args):
    """"""
    return _apply_column_function('st_aggregateDistanceSphere', *args)


def st_lengthSphere(*args):
    """"""
    return _apply_column_function('st_lengthSphere', *args)
