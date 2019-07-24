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
Module initialization for PyRasterFrames. This is where much of the cool stuff is
appended to PySpark classes.
"""

from __future__ import absolute_import
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, DataFrameReader
from pyspark.sql.column import _to_java_column

# Import RasterFrameLayer types and functions
from .rf_context import RFContext
from .version import __version__
from .rf_types import RasterFrameLayer, TileExploder, TileUDT, RasterSourceUDT

__all__ = ['RasterFrameLayer', 'TileExploder']


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


def get_spark_session():
    """ Create a SparkSession with pyrasterframes enabled and configured. """
    from pyrasterframes.utils import create_rf_spark_session

    return create_rf_spark_session()


def _convert_df(df, sp_key=None, metadata=None):
    ctx = SparkContext._active_spark_context._rf_context

    if sp_key is None:
        return RasterFrameLayer(ctx._jrfctx.asLayer(df._jdf), ctx._spark_session)
    else:
        import json
        return RasterFrameLayer(ctx._jrfctx.asLayer(
            df._jdf, _to_java_column(sp_key), json.dumps(metadata)), ctx._spark_session)


def _raster_join(df, other, left_extent=None, left_crs=None, right_extent=None, right_crs=None, join_exprs=None):
    ctx = SparkContext._active_spark_context._rf_context
    if join_exprs is not None:
        assert left_extent is not None and left_crs is not None and right_extent is not None and right_crs is not None
        # Note the order of arguments here.
        cols = [join_exprs, left_extent, left_crs, right_extent, right_crs]
        jdf = ctx._jrfctx.rasterJoin(df._jdf, other._jdf, *[_to_java_column(c) for c in cols])

    elif left_extent is not None:
        assert left_crs is not None and right_extent is not None and right_crs is not None
        cols = [left_extent, left_crs, right_extent, right_crs]
        jdf = ctx._jrfctx.rasterJoin(df._jdf, other._jdf, *[_to_java_column(c) for c in cols])

    else:
        jdf = ctx._jrfctx.rasterJoin(df._jdf, other._jdf)

    return RasterFrameLayer(jdf, ctx._spark_session)


def _layer_reader(df_reader, format_key, path, **options):
    """ Loads the file of the given type at the given path."""
    df = df_reader.format(format_key).load(path, **options)
    return _convert_df(df)


def _aliased_reader(df_reader, format_key, path, **options):
    """ Loads the file of the given type at the given path."""
    return df_reader.format(format_key).load(path, **options)

def _raster_reader(
        df_reader,
        path=None,
        catalog=None,
        catalog_col_names=None,
        band_indexes=None,
        tile_dimensions=(256, 256),
        lazy_tiles=True,
        **options):

    from pandas import DataFrame as PdDataFrame

    def to_csv(comp):
        if isinstance(comp, str):
            return comp
        else:
            return ','.join(str(v) for v in comp)

    def temp_name():
        """ Create a random name for a temporary view """
        import uuid
        return str(uuid.uuid4()).replace('-', '')

    if band_indexes is None:
        band_indexes = [0]

    options.update({
        "bandIndexes": to_csv(band_indexes),
        "tileDimensions": to_csv(tile_dimensions),
        "lazyTiles": lazy_tiles
    })

    if catalog is not None:
        if catalog_col_names is None:
            raise Exception("'catalog_col_names' required when DataFrame 'catalog' specified")
        if isinstance(catalog, str):
            options.update({
                "catalogCSV": catalog,
                "catalogColumns": to_csv(catalog_col_names)
            })
        elif isinstance(catalog, DataFrame):
            # Create a random view name
            tmp_name = temp_name()
            catalog.createOrReplaceTempView(tmp_name)
            options.update({
                "catalogTable": tmp_name,
                "catalogColumns": to_csv(catalog_col_names)
            })
        elif isinstance(catalog, PdDataFrame):
            # Handle to active spark session
            session = SparkContext._active_spark_context._rf_context._spark_session
            # Create a random view name
            tmp_name = temp_name()
            spark_catalog = session.createDataFrame(catalog)
            spark_catalog.createOrReplaceTempView(tmp_name)
            options.update({
                "catalogTable": tmp_name,
                "catalogColumns": to_csv(catalog_col_names)
            })

    return df_reader \
        .format("raster") \
        .load(path, **options)


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init
SparkSession.Builder.withKryoSerialization = _kryo_init

# Add the 'asLayer' method to pyspark DataFrame
DataFrame.as_layer = _convert_df

# Add `raster_join` method to pyspark DataFrame
DataFrame.raster_join = _raster_join

# Add DataSource convenience methods to the DataFrameReader
DataFrameReader.raster = _raster_reader
DataFrameReader.geojson = lambda df_reader, path: _aliased_reader(df_reader, "geojson", path)


# Legacy readers
DataFrameReader.geotiff = lambda df_reader, path: _layer_reader(df_reader, "geotiff", path)
DataFrameReader.geotrellis = lambda df_reader, path: _layer_reader(df_reader, "geotrellis", path)
