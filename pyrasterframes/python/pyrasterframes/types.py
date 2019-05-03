"""
This module contains all types relevant to PyRasterFrames. Classes in this module are
meant to provide smoother pathways between the jvm and Python, and whenever possible,
the implementations take advantage of the existing Scala functionality. The RasterFrame
class here provides the PyRasterFrames entry point.
"""

from pyspark.sql.types import UserDefinedType
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, Column, Row
from pyspark.sql.types import *
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from .context import RFContext
import numpy

__all__ = ['RasterFrame', 'TileUDT', 'RasterSourceUDT', 'TileExploder', 'NoDataFilter']


class RasterFrame(DataFrame):
    def __init__(self, jdf, spark_session):
        DataFrame.__init__(self, jdf, spark_session._wrapped)
        self._jrfctx = spark_session.rasterframes._jrfctx

    def tileColumns(self):
        """
        Fetches columns of type Tile.
        :return: One or more Column instances associated with Tiles.
        """
        cols = self._jrfctx.tileColumns(self._jdf)
        return [Column(c) for c in cols]

    def spatialKeyColumn(self):
        """
        Fetch the tagged spatial key column.
        :return: Spatial key column
        """
        col = self._jrfctx.spatialKeyColumn(self._jdf)
        return Column(col)

    def temporalKeyColumn(self):
        """
        Fetch the temporal key column, if any.
        :return: Temporal key column, or None.
        """
        col = self._jrfctx.temporalKeyColumn(self._jdf)
        return col and Column(col)

    def tileLayerMetadata(self):
        """
        Fetch the tile layer metadata.
        :return: A dictionary of metadata.
        """
        import json
        return json.loads(str(self._jrfctx.tileLayerMetadata(self._jdf)))

    def spatialJoin(self, other_df):
        """
        Spatially join this RasterFrame to the given RasterFrame.
        :return: Joined RasterFrame.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.spatialJoin(self._jdf, other_df._jdf)
        return RasterFrame(df, ctx._spark_session)

    def toIntRaster(self, colname, cols, rows):
        """
        Convert a tile to an Int raster
        :return: array containing values of the tile's cells
        """
        resArr = self._jrfctx.toIntRaster(self._jdf, colname, cols, rows)
        return resArr

    def toDoubleRaster(self, colname, cols, rows):
        """
        Convert a tile to an Double raster
        :return: array containing values of the tile's cells
        """
        resArr = self._jrfctx.toDoubleRaster(self._jdf, colname, cols, rows)
        return resArr

    def withBounds(self):
        """
        Add a column called "bounds" containing the extent of each row.
        :return: RasterFrame with "bounds" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withBounds(self._jdf)
        return RasterFrame(df, ctx._spark_session)

    def withCenter(self):
        """
        Add a column called "center" containing the center of the extent of each row.
        :return: RasterFrame with "center" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withCenter(self._jdf)
        return RasterFrame(df, ctx._spark_session)

    def withCenterLatLng(self):
        """
        Add a column called "center" containing the center of the extent of each row in Lat Long form.
        :return: RasterFrame with "center" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withCenterLatLng(self._jdf)
        return RasterFrame(df, ctx._spark_session)

    def withSpatialIndex(self):
        """
        Add a column containing the spatial index of each row.
        :return: RasterFrame with "center" column.
        """
        ctx = SparkContext._active_spark_context._rf_context
        df = ctx._jrfctx.withSpatialIndex(self._jdf)
        return RasterFrame(df, ctx._spark_session)


class RasterSourceUDT(UserDefinedType):
    @classmethod
    def sqlType(self):
        return StructType([
            StructField("raster_source_kryo", BinaryType(), False)])

    @classmethod
    def module(cls):
        return 'pyrasterframes.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.rf.RasterSourceUDT'

    def serialize(self, obj):
        # Not yet implemented. Kryo serialized bytes?
        return None

    def deserialize(self, datum):
        bytes(datum[0])


class Tile:

    def __init__(self, array, crs=None, extent=None, band_index=None, ):
        # this could be more flexible to allow more ways to access TileUDT
        # do some checking on dtype and shape
        self.array = array
        self.crs = crs  # what is a CRS? is it a rasterio.crs.CRS? A pyproj CRS? https://pyproj4.github.io/pyproj/html/api/crs.html#pyproj.crs.CRS.from_string
        self.extent = extent
        self.band_index = band_index
        # here may be okay to carry around the kryo serialized bytes so we can at least go back into the JVM world?:

    def __str__(self):
        return self.array.__str__()

    @classmethod
    def numpy_dtype_to_celltype(cls, dtype):
        from pyrasterframes.rasterfunctions import _celltype
        # TODO implement something that will work generally
        # Also here we should convert to a string representation of the celltype
        return _celltype(str(dtype))

    def get_cell_type(self):
        return self.numpy_dtype_to_celltype(self.array.dtype)

    def dimensions(self):
        # list of cols, rows as is conventional in GeoTrellis and RasterFrames
        return [self.array.shape[1], self.array.shape[0]]

    def to_tile_udt(self):
        row = [
            # cell_context
            [
                ['float64'],  #TODO !
                self.dimensions()
            ],
            # cell_data
            [
                # cells -- still too many copies
                bytearray(RFContext.call('list_to_bytearray', self.array.flatten().tolist(), *self.dimensions())),
                # ref
                [
                    # cell_data.ref.source
                    None,
                    # cell_data.ref.bandIndex
                    self.band_index,
                    # cell_data.ref.subextent
                    self.extent
                ]
            ]
        ]
        return row


class TileUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        """
        Mirrors `schema` in scala companion object org.apache.spark.sql.rf.TileUDT
        """
        return StructType([
            StructField("cell_context", StructType([
                StructField("cell_type", StructType([
                    StructField("cellTypeName", StringType(), False)
                ]), False),
                StructField("dimensions", StructType([
                    StructField("cols", ShortType(), False),
                    StructField("rows", ShortType(), False)
                ]), False),
            ]), False),
            StructField("cell_data", StructType([
                StructField("cells", BinaryType(), True),
                StructField("ref", StructType([
                    StructField("source", RasterSourceUDT(), False),
                    StructField("bandIndex", IntegerType(), False),
                    StructField("subextent", StructType([
                        StructField("xmin", DoubleType(), False),
                        StructField("ymin", DoubleType(), False),
                        StructField("xmax", DoubleType(), False),
                        StructField("ymax", DoubleType(), False)
                    ]), True)
                ]), True)
            ]), False)
        ])

    @classmethod
    def module(cls):
        return 'pyrasterframes.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.rf.TileUDT'

    def serialize(self, tile):
        return tile.to_tile_udt()

    def deserialize(self, datum):
        """

        :param datum:
        :return: A Tile object from row data.
        """
        cell_type = datum.cell_context.cellType.cellTypeName
        cols = datum.cell_context.dimensions.cols
        rows = datum.cell_context.dimensions.rows
        cell_data_bytes = datum.cell_data.cells
        cell_value_list = list(RFContext.call('bytearray_to_list', cell_data_bytes, cell_type, cols, rows))

        crs = None
        band_index = None
        extent = None
        if 'ref' in datum.cell_data and datum.cell_data.ref is not None:
            band_index = datum.cell_data.ref.bandIndex
            if datum.cell_data.ref.source is not None:
                crs = RFContext.call('rastersource_bytearray_to_proj4', datum.cell_data.ref.source)
            if datum.cell_data.ref.subextent is not None:
                extent = datum.cell_data.ref.subextent

        t = Tile(
            numpy.reshape(cell_value_list, (rows, cols), order='C').astype(cell_type),
            crs,
            extent,
            band_index,
        )
        return t

    deserialize.__safe_for_unpickling__ = True

Tile.__UDT__ = TileUDT()

class TileExploder(JavaTransformer, JavaMLReadable, JavaMLWritable):
    """
    Python wrapper for TileExploder.scala
    """
    def __init__(self):
        super(TileExploder, self).__init__()
        self._java_obj = self._new_java_obj("org.locationtech.rasterframes.ml.TileExploder", self.uid)

class NoDataFilter(JavaTransformer, JavaMLReadable, JavaMLWritable):
    """
    Python wrapper for NoDataFilter.scala
    """
    def __init__(self):
        super(NoDataFilter, self).__init__()
        self._java_obj = self._new_java_obj("org.locationtech.rasterframes.ml.NoDataFilter", self.uid)
    def setInputCols(self, values):
        self._java_obj.setInputCols(values)
