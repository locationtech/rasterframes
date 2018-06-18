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
from pyrasterframes.context import _checked_context

__all__ = ['RFContext', 'RasterFrame', 'TileUDT', 'TileExploder']

class RFContext(object):
    """
    Entrypoint to RasterFrames services
    """
    def __init__(self, spark_session):
        self._spark_session = spark_session
        self._gateway = spark_session.sparkContext._gateway
        self._jvm = self._gateway.jvm
        jsess = self._spark_session._jsparkSession
        self._jrfctx = self._jvm.astraea.spark.rasterframes.py.PyRFContext(jsess)


class RasterFrame(DataFrame):
    def __init__(self, jdf, spark_session):
        DataFrame.__init__(self, jdf, spark_session)
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


class TileUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: Internal use only.
    """

    @classmethod
    def sqlType(self):
        return StructType([
            StructField("cellType", StringType(), False),
            StructField("cols", ShortType(), False),
            StructField("rows", ShortType(), False),
            StructField("data", BinaryType(), False)
        ])

    @classmethod
    def module(cls):
        return 'pyrasterframes'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.gt.types.TileUDT'

    def serialize(self, obj):
        if (obj is None): return None
        return Row(obj.cellType().name().encode("UTF8"),
                  obj.cols().toShort(),
                  obj.rows().toShort(),
                  obj.toBytes)

    def deserialize(self, datum):
        return _checked_context().generateTile(datum[0], datum[1], datum[2], datum[3])



class TileExploder(JavaTransformer, JavaMLReadable, JavaMLWritable):

    def __init__(self):
        super(TileExploder, self).__init__()
        self._java_obj = self._new_java_obj("astraea.spark.rasterframes.ml.TileExploder", self.uid)