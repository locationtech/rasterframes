"""
Module initialization for PyRasterFrames. This is where much of the cool stuff is
appended to PySpark classes.
"""

from __future__ import absolute_import
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, DataFrameReader
from pyspark.sql.column import _to_java_column

# Import RasterFrame types and functions
from .rf_types import *
from . import rasterfunctions
from .context import RFContext

__all__ = ['RasterFrame', 'TileExploder']


def _rf_init(spark_session):
    """ Adds RasterFrames functionality to PySpark session."""
    if not hasattr(spark_session, "rasterframes"):
        spark_session.rasterframes = RFContext(spark_session)
        spark_session.sparkContext._rf_context = spark_session.rasterframes

    return spark_session


def _kryo_init(builder):
    """Registers Kryo Serializers for better performance."""
    # NB: These methods need to be kept up-to-date wit those in `org.locationtech.rasterframes.extensions.KryoMethods`
    builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.locationtech.rasterframes.util.RFKryoRegistrator") \
        .config("spark.kryoserializer.buffer.max", "500m")
    return builder


def _convert_df(df, sp_key=None, metadata=None):
    ctx = SparkContext._active_spark_context._rf_context

    if sp_key is None:
        return RasterFrame(ctx._jrfctx.asRF(df._jdf), ctx._spark_session)
    else:
        import json
        return RasterFrame(ctx._jrfctx.asRF(
            df._jdf, _to_java_column(sp_key), json.dumps(metadata)), ctx._spark_session)


def _layer_reader(df_reader, format_key, path, **options):
    """ Loads the file of the given type at the given path."""
    df = df_reader.format(format_key).load(path, **options)
    return _convert_df(df)


def _rastersource_reader(df_reader, path=None, band_indexes=None, tile_dimensions=(256, 256), **options):
    if band_indexes is None:
        band_indexes = [0]

    def to_csv(comp): return ','.join(str(v) for v in comp)
    # If path is list, convert to newline delimited string and pass as "paths" (plural)
    options.update({
        "bandIndexes": to_csv(band_indexes),
        "tileDimensions": to_csv(tile_dimensions)
    })
    return df_reader \
        .format("rastersource") \
        .load(path, **options)


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init
SparkSession.Builder.withKryoSerialization = _kryo_init

# Add the 'asRF' method to pyspark DataFrame
DataFrame.asRF = _convert_df

# Add DataSource convenience methods to the DataFrameReader
DataFrameReader.rastersource = _rastersource_reader

# Legacy readers
DataFrameReader.geotiff = lambda df_reader, path: _layer_reader(df_reader, "geotiff", path)
DataFrameReader.geotrellis = lambda df_reader, path: _layer_reader(df_reader, "geotrellis", path)
