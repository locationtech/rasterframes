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
    return Column(jfcn(_to_java_column(col_index), _to_java_column(row_index), _to_java_column(cell_data_col), num_cols, num_rows, _parse_cell_type(cell_type_str)))


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
    return Column(jfcn(_to_java_column(geometry_col), _to_java_column(bounds_col), _to_java_column(value_col), num_cols, num_rows))


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


_rf_column_functions = {
    # ------- RasterFrames functions -------
    'rf_dimensions': 'Query the number of (cols, rows) in a Tile.',
    'rf_tile_to_array_int': 'Flattens Tile into an array of integers.',
    'rf_tile_to_array_double': 'Flattens Tile into an array of doubles.',
    'rf_cell_type': 'Extract the Tile\'s cell type',
    'rf_is_no_data_tile': 'Report if the Tile is entirely NODDATA cells',
    'rf_exists': 'Returns true if any cells in the tile are true (non-zero and not NoData)',
    'rf_for_all': 'Returns true if all cells in the tile are true (non-zero and not NoData).',
    'rf_agg_approx_histogram': 'Compute the full column aggregate floating point histogram',
    'rf_agg_stats': 'Compute the full column aggregate floating point statistics',
    'rf_agg_mean': 'Computes the column aggregate mean',
    'rf_agg_data_cells': 'Computes the number of non-NoData cells in a column',
    'rf_agg_no_data_cells': 'Computes the number of NoData cells in a column',
    'rf_tile_histogram': 'Compute the Tile-wise histogram',
    'rf_tile_mean': 'Compute the Tile-wise mean',
    'rf_tile_sum': 'Compute the Tile-wise sum',
    'rf_tile_min': 'Compute the Tile-wise minimum',
    'rf_tile_max': 'Compute the Tile-wise maximum',
    'rf_tile_stats': 'Compute the Tile-wise floating point statistics',
    'rf_render_ascii': 'Render ASCII art of tile',
    'rf_render_matrix': 'Render Tile cell values as numeric values, for debugging purposes',
    'rf_no_data_cells': 'Count of NODATA cells',
    'rf_data_cells': 'Count of cells with valid data',
    'rf_local_add': 'Add two Tiles',
    'rf_local_subtract': 'Subtract two Tiles',
    'rf_local_multiply': 'Multiply two Tiles',
    'rf_local_divide': 'Divide two Tiles',
    'rf_normalized_difference': 'Compute the normalized difference of two tiles',
    'rf_agg_local_max': 'Compute the cell-wise/local max operation between Tiles in a column.',
    'rf_agg_local_min': 'Compute the cellwise/local min operation between Tiles in a column.',
    'rf_agg_local_mean': 'Compute the cellwise/local mean operation between Tiles in a column.',
    'rf_agg_local_data_cells': 'Compute the cellwise/local count of non-NoData cells for all Tiles in a column.',
    'rf_agg_local_no_data_cells': 'Compute the cellwise/local count of NoData cells for all Tiles in a column.',
    'rf_agg_local_stats': 'Compute cell-local aggregate descriptive statistics for a column of Tiles.',
    'rf_mask': 'Where the rf_mask (second) tile contains NODATA, replace values in the source (first) tile with NODATA.',
    'rf_inverse_mask': 'Where the rf_mask (second) tile DOES NOT contain NODATA, replace values in the source (first) tile with NODATA.',
    'rf_local_less': 'Cellwise less than comparison between two tiles',
    'rf_local_less_equal': 'Cellwise less than or equal to comparison between two tiles',
    'rf_local_greater': 'Cellwise greater than comparison between two tiles',
    'rf_local_greater_equal': 'Cellwise greater than or equal to comparison between two tiles',
    'rf_local_equal': 'Cellwise equality comparison between two tiles',
    'rf_local_unequal': 'Cellwise inequality comparison between two tiles',
    'rf_round': 'Round cell values to the nearest integer without changing the cell type',
    'rf_abs': 'Compute the absolute value of each cell',
    'rf_log': 'Performs cell-wise natural logarithm',
    'rf_log10': 'Performs cell-wise logartithm with base 10',
    'rf_log2': 'Performs cell-wise logartithm with base 2',
    'rf_log1p': 'Performs natural logarithm of cell values plus one',
    'rf_exp': 'Performs cell-wise exponential',
    'rf_exp2': 'Compute 2 to the power of cell values',
    'rf_exp10': 'Compute 10 to the power of cell values',
    'rf_expm1': 'Performs cell-wise exponential, then subtract one',
    'rf_identity': 'Pass tile through unchanged',
    'rf_resample': 'Resample tile to different size based on scalar factor or tile whose dimension to match',
    'rf_crs': 'Get the CRS of a RasterSource or ProjectedRasterTile',
    'st_extent': 'Compute the extent/bbox of a Geometry (a tile with embedded extent and CRS)',
    'rf_extent': 'Get the extent of a RasterSource or ProjectedRasterTile (a tile with embedded extent and CRS)',
    'rf_tile': 'Extracts the Tile component of a ProjectedRasterTile (or Tile).',
    'st_geometry': 'Convert the given extent/bbox to a polygon',
    'rf_geometry': 'Get the extent of a RasterSource or ProjectdRasterTile as a Geometry',

    # ------- JTS functions -------
    # spatial constructors
    'st_geomFromGeoHash': '',
    'st_geomFromWKT': '',
    'st_geomFromWKB': '',
    'st_lineFromText': '',
    'st_makeBox2D': '',
    'st_makeBBox': '',
    'st_makePolygon': '',
    'st_makePoint': '',
    'st_makeLine': '',
    'st_makePointM': '',
    'st_mLineFromText': '',
    'st_mPointFromText': '',
    'st_mPolyFromText': '',
    'st_point': '',
    'st_pointFromGeoHash': '',
    'st_pointFromText': '',
    'st_pointFromWKB': '',
    'st_polygon': '',
    'st_polygonFromText': '',
    # spatial converters
    'st_castToPoint': '',
    'st_castToPolygon': '',
    'st_castToLineString': '',
    'st_byteArray': '',
    # spatial accessors
    'st_boundary': '',
    'st_coordDim': '',
    'st_dimension': '',
    'st_envelope': '',
    'st_exteriorRing': '',
    'st_geometryN': '',
    'st_geometryType': '',
    'st_interiorRingN': '',
    'st_isClosed': '',
    'st_isCollection': '',
    'st_isEmpty': '',
    'st_isRing': '',
    'st_isSimple': '',
    'st_isValid': '',
    'st_numGeometries': '',
    'st_numPoints': '',
    'st_pointN': '',
    'st_x': '',
    'st_y': '',
    # spatial outputs
    'st_asBinary': '',
    'st_asGeoJSON': '',
    'st_asLatLonText': '',
    'st_asText': '',
    'st_geoHash': '',
    # spatial processors
    'st_bufferPoint': '',
    'st_antimeridianSafeGeom': '',
    # spatial relations
    'st_translate': '',
    'st_contains': '',
    'st_covers': '',
    'st_crosses': '',
    'st_disjoint': '',
    'st_equals': '',
    'st_intersects': '',
    'st_overlaps': '',
    'st_touches': '',
    'st_within': '',
    'st_relate': '',
    'st_relateBool': '',
    'st_area': '',
    'st_closestPoint': '',
    'st_centroid': '',
    'st_distance': '',
    'st_distanceSphere': '',
    'st_length': '',
    'st_aggregateDistanceSphere': '',
    'st_lengthSphere': '',
}


def _create_column_function(name, doc=""):
    """ Create a mapping to Scala UDF for a column function by name"""
    def _(*args):
        jfcn = RFContext.active().lookup(name)
        jcols = [_to_java_column(arg) for arg in args]
        return Column(jfcn(*jcols))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = THIS_MODULE
    return _



def _register_functions():
    """ Register each function in the scope"""
    for name, doc in _rf_column_functions.items():
        globals()[name] = _create_column_function(name, doc)


_register_functions()