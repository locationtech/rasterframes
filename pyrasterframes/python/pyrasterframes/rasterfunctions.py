"""
This module creates explicit Python functions that map back to the existing Scala
implementations. Most functions are standard Column functions, but those with unique
signatures are handled here as well.
"""


from __future__ import absolute_import
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column
from .context import _checked_context


THIS_MODULE = 'pyrasterframes'


def _context_call(name, *args):
    f = getattr(_checked_context(), name)
    return f(*args)


def _celltype(cellTypeStr):
    """ Convert the string cell type to the expected CellType object."""
    return _context_call('cellType', cellTypeStr)


def _create_assembleTile():
    """ Create a function mapping to the Scala implementation."""
    def _(colIndex, rowIndex, cellData, numCols, numRows, cellType):
        ctx = _checked_context()
        jfcn = getattr(ctx, 'assembleTile')
        return Column(jfcn(_to_java_column(colIndex), _to_java_column(rowIndex), _to_java_column(cellData), numCols, numRows, _celltype(cellType)))
    _.__name__ = 'assembleTile'
    _.__doc__ = "Create a Tile from  a column of cell data with location indices"
    _.__module__ = THIS_MODULE
    return _


def _create_arrayToTile():
    """ Create a function mapping to the Scala implementation."""
    def _(arrayCol, numCols, numRows):
        jfcn = getattr(_checked_context(), 'arrayToTile')
        return Column(jfcn(_to_java_column(arrayCol), numCols, numRows))
    _.__name__ = 'arrayToTile'
    _.__doc__ = "Convert array in `arrayCol` into a Tile of dimensions `numCols` and `numRows'"
    _.__module__ = THIS_MODULE
    return _


def _create_convertCellType():
    """ Create a function mapping to the Scala implementation."""
    def _(tileCol, cellType):
        jfcn = getattr(_checked_context(), 'convertCellType')
        return Column(jfcn(_to_java_column(tileCol), _celltype(cellType)))
    _.__name__ = 'convertCellType'
    _.__doc__ = "Convert the numeric type of the Tiles in `tileCol`"
    _.__module__ = THIS_MODULE
    return _


def _create_makeConstantTile():
    """ Create a function mapping to the Scala implementation."""
    def _(value, cols, rows, cellType):
        jfcn = getattr(_checked_context(), 'makeConstantTile')
        return Column(jfcn(value, cols, rows, cellType))
    _.__name__ = 'makeConstantTile'
    _.__doc__ = "Constructor for constant tile column"
    _.__module__ = THIS_MODULE
    return _


def _create_tileZeros():
    """ Create a function mapping to the Scala implementation."""
    def _(cols, rows, cellType = 'float64'):
        jfcn = getattr(_checked_context(), 'tileZeros')
        return Column(jfcn(cols, rows, cellType))
    _.__name__ = 'tileZeros'
    _.__doc__ = "Create column of constant tiles of zero"
    _.__module__ = THIS_MODULE
    return _


def _create_tileOnes():
    """ Create a function mapping to the Scala implementation."""
    def _(cols, rows, cellType = 'float64'):
        jfcn = getattr(_checked_context(), 'tileOnes')
        return Column(jfcn(cols, rows, cellType))
    _.__name__ = 'tileOnes'
    _.__doc__ = "Create column of constant tiles of one"
    _.__module__ = THIS_MODULE
    return _


def _create_rasterize():
    """ Create a function mapping to the Scala rasterize function. """
    def _(geometryCol, boundsCol, valueCol, numCols, numRows):
        jfcn = getattr(_checked_context(), 'rasterize')
        return Column(jfcn(_to_java_column(geometryCol), _to_java_column(boundsCol), _to_java_column(valueCol), numCols, numRows))
    _.__name__ = 'rasterize'
    _.__doc__ = 'Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value.'
    _.__module__ = THIS_MODULE
    return _

def _create_reproject_geometry():
    """ Create a function mapping to the Scala reprojectGeometry function. """
    def _(geometryCol, srcCRSName, dstCRSName):
        jfcn = getattr(_checked_context(), 'reprojectGeometry')
        return Column(jfcn(_to_java_column(geometryCol), srcCRSName, dstCRSName))
    _.__name__ = 'reprojectGeometry'
    _.__doc__ = """Reproject a column of geometry given the CRS names of the source and destination.
Currently supported registries are EPSG, ESRI, WORLD, NAD83, & NAD27.
An example of a valid CRS name is EPSG:3005.
"""
    _.__module__ = THIS_MODULE
    return _

_rf_unique_functions = {
    'assembleTile': _create_assembleTile(),
    'arrayToTile': _create_arrayToTile(),
    'convertCellType': _create_convertCellType(),
    'makeConstantTile': _create_makeConstantTile(),
    'tileZeros': _create_tileZeros(),
    'tileOnes': _create_tileOnes(),
    'cellTypes': lambda: _context_call('cellTypes'),
    'rasterize': _create_rasterize(),
    'reprojectGeometry': _create_reproject_geometry()
}


_rf_column_scalar_functions = {
    'withNoData': 'Assign a `NoData` value to the Tiles in the given Column.',
    'localAddScalar': 'Add a scalar to a Tile',
    'localSubtractScalar': 'Subtract a scalar from a Tile',
    'localMultiplyScalar': 'Multiply a Tile by a scalar',
    'localDivideScalar': 'Divide a Tile by a scalar',
    'localLessScalar': 'Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0',
    'localLessEqualScalar': 'Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0',
    'localGreaterScalar': 'Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0',
    'localGreaterEqualScalar': 'Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0',
    'localEqualScalar': 'Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0',
    'localUnequalScalar': 'Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0',
}


_rf_column_functions = {
    # ------- RasterFrames functions -------
    'explodeTiles': 'Create a row for each cell in Tile.',
    'tileDimensions': 'Query the number of (cols, rows) in a Tile.',
    'envelope': 'Extracts the bounding box (envelope) of the geometry.',
    'tileToIntArray': 'Flattens Tile into an array of integers.',
    'tileToDoubleArray': 'Flattens Tile into an array of doubles.',
    'cellType': 'Extract the Tile\'s cell type',
    'aggHistogram': 'Compute the full column aggregate floating point histogram',
    'aggStats': 'Compute the full column aggregate floating point statistics',
    'aggMean': 'Computes the column aggregate mean',
    'aggDataCells': 'Computes the number of non-NoData cells in a column',
    'aggNoDataCells': 'Computes the number of NoData cells in a column',
    'tileHistogram': 'Compute the Tile-wise histogram',
    'tileMean': 'Compute the Tile-wise mean',
    'tileSum': 'Compute the Tile-wise sum',
    'tileMin': 'Compute the Tile-wise minimum',
    'tileMax': 'Compute the Tile-wise maximum',
    'tileStats': 'Compute the Tile-wise floating point statistics',
    'renderAscii': 'Render ASCII art of tile',
    'noDataCells': 'Count of NODATA cells',
    'dataCells': 'Count of cells with valid data',
    'localAdd': 'Add two Tiles',
    'localSubtract': 'Subtract two Tiles',
    'localMultiply': 'Multiply two Tiles',
    'localDivide': 'Divide two Tiles',
    'normalizedDifference': 'Compute the normalized difference of two tiles',
    'localAggStats': 'Compute cell-local aggregate descriptive statistics for a column of Tiles.',
    'localAggMax': 'Compute the cell-wise/local max operation between Tiles in a column.',
    'localAggMin': 'Compute the cellwise/local min operation between Tiles in a column.',
    'localAggMean': 'Compute the cellwise/local mean operation between Tiles in a column.',
    'localAggDataCells': 'Compute the cellwise/local count of non-NoData cells for all Tiles in a column.',
    'localAggNoDataCells': 'Compute the cellwise/local count of NoData cells for all Tiles in a column.',
    'mask': 'Where the mask (second) tile contains NODATA, replace values in the source (first) tile with NODATA.',
    'inverseMask': 'Where the mask (second) tile DOES NOT contain NODATA, replace values in the source (first) tile with NODATA.',
    'localLess': 'Cellwise less than comparison between two tiles',
    'localLessEqual': 'Cellwise less than or equal to comparison between two tiles',
    'localGreater': 'Cellwise greater than comparison between two tiles',
    'localGreaterEqual': 'Cellwise greater than or equal to comparison between two tiles',
    'localEqual': 'Cellwise equality comparison between two tiles',
    'localUnequal': 'Cellwise inequality comparison between two tiles',
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
        jfcn = getattr(_checked_context(), name)
        jcols = [_to_java_column(arg) for arg in args]
        return Column(jfcn(*jcols))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = THIS_MODULE
    return _


def _create_columnScalarFunction(name, doc=""):
    """ Create a mapping to Scala UDF for a (column, scalar) -> column function by name"""
    def _(col, scalar):
        jfcn = getattr(_checked_context(), name)
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