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
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column
from .context import RFContext
from .rf_types import CellType

THIS_MODULE = 'pyrasterframes'


def _context_call(name, *args):
    f = RFContext.active().lookup(name)
    return f(*args)


def _celltype(cellTypeStr):
    """ Convert the string cell type to the expected CellType object."""
    return _context_call('rf_cell_type', cellTypeStr)


def _create_assembleTile():
    """ Create a function mapping to the Scala implementation."""
    def _(colIndex, rowIndex, cellData, numCols, numRows, cellType):
        jfcn = RFContext.active().lookup('rf_assemble_tile')
        return Column(jfcn(_to_java_column(colIndex), _to_java_column(rowIndex), _to_java_column(cellData), numCols, numRows, _celltype(cellType)))
    _.__name__ = 'rf_assemble_tile'
    _.__doc__ = "Create a Tile from  a column of cell data with location indices"
    _.__module__ = THIS_MODULE
    return _


def _create_arrayToTile():
    """ Create a function mapping to the Scala implementation."""
    def _(arrayCol, numCols, numRows):
        jfcn = RFContext.active().lookup('rf_array_to_tile')
        return Column(jfcn(_to_java_column(arrayCol), numCols, numRows))
    _.__name__ = 'rf_array_to_tile'
    _.__doc__ = "Convert array in `arrayCol` into a Tile of dimensions `numCols` and `numRows'"
    _.__module__ = THIS_MODULE
    return _


def _create_convertCellType():
    """ Create a function mapping to the Scala implementation."""
    def _(tileCol, cellType):
        jfcn = RFContext.active().lookup('rf_convert_cell_type')
        return Column(jfcn(_to_java_column(tileCol), _celltype(cellType)))
    _.__name__ = 'rf_convert_cell_type'
    _.__doc__ = "Convert the numeric type of the Tiles in `tileCol`"
    _.__module__ = THIS_MODULE
    return _


def _create_makeConstantTile():
    """ Create a function mapping to the Scala implementation."""
    def _(value, cols, rows, cellType):
        jfcn = RFContext.active().lookup('rf_make_constant_tile')
        return Column(jfcn(value, cols, rows, cellType))
    _.__name__ = 'rf_make_constant_tile'
    _.__doc__ = "Constructor for constant tile column"
    _.__module__ = THIS_MODULE
    return _


def _create_tileZeros():
    """ Create a function mapping to the Scala implementation."""
    def _(cols, rows, cellType = 'float64'):
        jfcn = RFContext.active().lookup('rf_make_zeros_tile')
        return Column(jfcn(cols, rows, cellType))
    _.__name__ = 'rf_make_zeros_tile'
    _.__doc__ = "Create column of constant tiles of zero"
    _.__module__ = THIS_MODULE
    return _


def _create_tileOnes():
    """ Create a function mapping to the Scala implementation."""
    def _(cols, rows, cellType = 'float64'):
        jfcn = RFContext.active().lookup('rf_make_ones_tile')
        return Column(jfcn(cols, rows, cellType))
    _.__name__ = 'rf_tile_ones'
    _.__doc__ = "Create column of constant tiles of one"
    _.__module__ = THIS_MODULE
    return _


def _create_rasterize():
    """ Create a function mapping to the Scala rf_rasterize function. """
    def _(geometryCol, boundsCol, valueCol, numCols, numRows):
        jfcn = RFContext.active().lookup('rf_rasterize')
        return Column(jfcn(_to_java_column(geometryCol), _to_java_column(boundsCol), _to_java_column(valueCol), numCols, numRows))
    _.__name__ = 'rf_rasterize'
    _.__doc__ = 'Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value.'
    _.__module__ = THIS_MODULE
    return _


def _create_reproject_geometry():
    """ Create a function mapping to the Scala st_reproject function. """
    def _(geometryCol, srcCRSName, dstCRSName):
        jfcn = RFContext.active().lookup('st_reproject')
        return Column(jfcn(_to_java_column(geometryCol), srcCRSName, dstCRSName))
    _.__name__ = 'st_reproject'
    _.__doc__ = """Reproject a column of geometry given the CRS names of the source and destination.
Currently supported registries are EPSG, ESRI, WORLD, NAD83, & NAD27.
An example of a valid CRS name is EPSG:3005.
"""
    _.__module__ = THIS_MODULE
    return _


def _create_explode_tiles():
    """ Create a function mapping to Scala rf_explode_tiles function """
    def _(*args):
        jfcn = RFContext.active().lookup('rf_explode_tiles')
        jcols = [_to_java_column(arg) for arg in args]
        return Column(jfcn(RFContext.active().list_to_seq(jcols)))
    _.__name__ = 'rf_explode_tiles'
    _.__doc__ = 'Create a row for each cell in Tile.'
    _.__module__ = THIS_MODULE
    return _


def _create_explode_tiles_sample():
    """ Create a function mapping to Scala rf_explode_tiles_sample function"""
    def _(sample_frac, seed, *tile_cols):
        jfcn = RFContext.active().lookup('rf_explode_tiles_sample')
        jcols = [_to_java_column(arg) for arg in tile_cols]
        return Column(jfcn(sample_frac, seed, RFContext.active().list_to_seq(jcols)))

    _.__name__ = 'rf_explode_tiles_sample'
    _.__doc__ = 'Create a row for a sample of cells in Tile columns.'
    _.__module__ = THIS_MODULE
    return _


def _create_maskByValue():
    """ Create a function mapping to Scala rf_mask_by_value function """
    def _(data_tile, mask_tile, mask_value):
        jfcn = RFContext.active().lookup('rf_mask_by_value')
        return Column(jfcn(_to_java_column(data_tile), _to_java_column(mask_tile), _to_java_column(mask_value)))
    _.__name__ = 'rf_mask_by_value'
    _.__doc__ = 'Generate a tile with the values from the data tile, but where cells in the masking tile contain the masking value, replace the data value with NODATA.'
    _.__module__ = THIS_MODULE
    return _

def _create_inverseMaskByValue():
    """ Create a function mapping to Scala rf_inverse_mask_by_value function """
    def _(data_tile, mask_tile, mask_value):
        jfcn = RFContext.active().lookup('rf_inverse_mask_by_value')
        return Column(jfcn(_to_java_column(data_tile), _to_java_column(mask_tile), _to_java_column(mask_value)))
    _.__name__ = 'rf_inverse_mask_by_value'
    _.__doc__ = 'Generate a tile with the values from the data tile, but where cells in the masking tile do not contain the masking value, replace the data value with NODATA.'
    _.__module__ = THIS_MODULE
    return _

_rf_unique_functions = {
    'rf_array_to_tile': _create_arrayToTile(),
    'rf_assemble_tile': _create_assembleTile(),
    'rf_cell_types': lambda: [CellType(str(ct)) for ct in _context_call('rf_cell_types')],
    'rf_convert_cell_type': _create_convertCellType(),
    'rf_explode_tiles': _create_explode_tiles(),
    'rf_explode_tiles_sample': _create_explode_tiles_sample(),
    'rf_make_constant_tile': _create_makeConstantTile(),
    'rf_mask_by_value': _create_maskByValue(),
    'rf_inverse_mask_by_value': _create_inverseMaskByValue(),
    'rf_rasterize': _create_rasterize(),
    'st_reproject': _create_reproject_geometry(),
    'rf_make_ones_tile': _create_tileOnes(),
    'rf_make_zeros_tile': _create_tileZeros(),
}


_rf_column_scalar_functions = {
    'rf_with_no_data': 'Assign a `NoData` value to the Tiles in the given Column.',
    'rf_local_add_double': 'Add a scalar to a Tile',
    'rf_local_add_int': 'Add a scalar to a Tile',
    'rf_local_subtract_double': 'Subtract a scalar from a Tile',
    'rf_local_subtract_int': 'Subtract a scalar from a Tile',
    'rf_local_multiply_double': 'Multiply a Tile by a scalar',
    'rf_local_multiply_int': 'Multiply a Tile by a scalar',
    'rf_local_divide_double': 'Divide a Tile by a scalar',
    'rf_local_divide_int': 'Divide a Tile by a scalar',
    'rf_local_less_double': 'Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0',
    'rf_local_less_int': 'Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0',
    'rf_local_less_equal_double': 'Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0',
    'rf_local_less_equal_int': 'Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0',
    'rf_local_greater_double': 'Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0',
    'rf_local_greater_int': 'Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0',
    'rf_local_greater_equal_double': 'Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0',
    'rf_local_greater_equal_int': 'Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0',
    'rf_local_equal_double': 'Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0',
    'rf_local_equal_int': 'Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0',
    'rf_local_unequal_double': 'Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0',
    'rf_local_unequal_int': 'Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0',
}


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


__all__ = list(_rf_column_functions.keys()) + \
          list(_rf_column_scalar_functions.keys()) + \
          list(_rf_unique_functions.keys())


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


def _create_columnScalarFunction(name, doc=""):
    """ Create a mapping to Scala UDF for a (column, scalar) -> column function by name"""
    def _(col, scalar):
        jfcn = RFContext.active().lookup(name)
        return Column(jfcn(_to_java_column(col), scalar))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = THIS_MODULE
    return _


def _register_functions():
    """ Register each function in the scope"""
    for name, doc in _rf_column_functions.items():
        globals()[name] = _create_column_function(name, doc)

    for name, doc in _rf_column_scalar_functions.items():
        globals()[name] = _create_columnScalarFunction(name, doc)

    for name, func in _rf_unique_functions.items():
        globals()[name] = func


_register_functions()