"""
Module initialization for PyRasterFrames. This is where much of the cool stuff is
appended to PySpark classes.
"""


from __future__ import absolute_import
from pyspark.sql.types import UserDefinedType
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.column import _to_java_column

# Import RasterFrame types and functions
from pyrasterframes.types import *
from pyrasterframes import rasterfunctions


__all__ = ['RasterFrame', 'TileExploder']


def _rf_init(spark_session):
    """ Adds RasterFrames functionality to PySpark session."""
    if not hasattr(spark_session, "rasterframes"):
        spark_session.rasterframes = RFContext(spark_session)
        spark_session.sparkContext._rf_context = spark_session.rasterframes
    return spark_session


def _reader(df_reader, format_key, path, **options):
    """ Loads the file of the given type at the given path."""
    df = df_reader.format(format_key).load(path, **options)
    return _convertDF(df)


def _convertDF(df, sp_key = None, metadata = None):
    ctx = SparkContext._active_spark_context._rf_context

    if sp_key is None:
        return RasterFrame(ctx._jrfctx.asRF(df._jdf), ctx._spark_session)
    else:
        import json
        return RasterFrame(ctx._jrfctx.asRF(
            df._jdf, _to_java_column(sp_key), json.dumps(metadata)), ctx._spark_session)


_prevFJ = UserDefinedType.fromJson
def _fromJson(json_val):
    if str(json_val['class']).startswith('org.apache.spark.sql.jts'):
        json_val['pyClass'] = 'pyrasterframes.GeometryUDT'

    return _prevFJ(json_val)


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

# Add the 'asRF' method to pyspark DataFrame
DataFrame.asRF = _convertDF

# Add DataSource convenience methods to the DataFrameReader
# TODO: make sure this supports **options
DataFrameReader.geotiff = lambda df_reader, path: _reader(df_reader, "geotiff", path)
DataFrameReader.geotrellis = lambda df_reader, path: _reader(df_reader, "geotrellis", path)

# If you don't have Python support, you will get it anyway
UserDefinedType.fromJson = _fromJson

