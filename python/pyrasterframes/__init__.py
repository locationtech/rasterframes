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

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql.column import _to_java_column

# Import RasterFrameLayer types and functions
from .rf_context import RFContext
from .version import __version__
from .rf_types import RasterFrameLayer, TileExploder, TileUDT, RasterSourceUDT
import geomesa_pyspark.types  # enable vector integrations

from typing import Dict, Tuple, List, Optional, Union

__all__ = ['RasterFrameLayer', 'TileExploder']


def _rf_init(spark_session: SparkSession) -> SparkSession:
    """ Adds RasterFrames functionality to PySpark session."""
    if not hasattr(spark_session, "rasterframes"):
        spark_session.rasterframes = RFContext(spark_session)
        spark_session.sparkContext._rf_context = spark_session.rasterframes

    return spark_session


def _kryo_init(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Registers Kryo Serializers for better performance."""
    # NB: These methods need to be kept up-to-date wit those in `org.locationtech.rasterframes.extensions.KryoMethods`
    builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.locationtech.rasterframes.util.RFKryoRegistrator") \
        .config("spark.kryoserializer.buffer.max", "500m")
    return builder


def _convert_df(df: DataFrame, sp_key=None, metadata=None) -> RasterFrameLayer:
    """ Internal function to convert a DataFrame to a RasterFrameLayer. """
    ctx = SparkContext._active_spark_context._rf_context

    if sp_key is None:
        return RasterFrameLayer(ctx._jrfctx.asLayer(df._jdf), ctx._spark_session)
    else:
        import json
        return RasterFrameLayer(ctx._jrfctx.asLayer(
            df._jdf, _to_java_column(sp_key), json.dumps(metadata)), ctx._spark_session)


def _raster_join(df: DataFrame, other: DataFrame,
                 left_extent=None, left_crs=None,
                 right_extent=None, right_crs=None,
                 join_exprs=None, resampling_method='nearest_neighbor') -> DataFrame:
    ctx = SparkContext._active_spark_context._rf_context
    resampling_method = resampling_method.lower().strip().replace('_', '')
    assert resampling_method in ['nearestneighbor', 'bilinear', 'cubicconvolution', 'cubicspline', 'lanczos',
                                 'average', 'mode', 'median', 'max', 'min', 'sum']
    if join_exprs is not None:
        assert left_extent is not None and left_crs is not None and right_extent is not None and right_crs is not None
        # Note the order of arguments here.
        cols = [join_exprs, left_extent, left_crs, right_extent, right_crs]
        args = [_to_java_column(c) for c in cols] + [resampling_method]
        jdf = ctx._jrfctx.rasterJoin(df._jdf, other._jdf, *args)

    elif left_extent is not None:
        assert left_crs is not None and right_extent is not None and right_crs is not None
        cols = [left_extent, left_crs, right_extent, right_crs]
        args = [_to_java_column(c) for c in cols] + [resampling_method]
        jdf = ctx._jrfctx.rasterJoin(df._jdf, other._jdf, *args)

    else:
        jdf = ctx._jrfctx.rasterJoin(df._jdf, other._jdf, resampling_method)

    return DataFrame(jdf, ctx._spark_session._wrapped)


def _layer_reader(df_reader: DataFrameReader, format_key: str, path: Optional[str], **options: str) -> RasterFrameLayer:
    """ Loads the file of the given type at the given path."""
    df = df_reader.format(format_key).load(path, **options)
    return _convert_df(df)


def _aliased_reader(df_reader: DataFrameReader, format_key: str, path: Optional[str], **options: str) -> DataFrame:
    """ Loads the file of the given type at the given path."""
    return df_reader.format(format_key).load(path, **options)


def _aliased_writer(df_writer: DataFrameWriter, format_key: str, path: Optional[str], **options: str):
    """ Saves the dataframe to a file of the given type at the given path."""
    return df_writer.format(format_key).save(path, **options)


def _raster_reader(
        df_reader: DataFrameReader,
        source=None,
        catalog_col_names: Optional[List[str]] = None,
        band_indexes: Optional[List[int]] = None,
        buffer_size: int = 0,
        tile_dimensions: Tuple[int] = (256, 256),
        lazy_tiles: bool = True,
        spatial_index_partitions=None,
        **options: str) -> DataFrame:
    """
    Returns a Spark DataFrame from raster data files specified by URIs.
    Each row in the returned DataFrame will contain a column with struct of (CRS, Extent, Tile) for each item in
      `catalog_col_names`.
    Multiple bands from the same raster file are spread across rows of the DataFrame. See `band_indexes` param.
    If bands from a scene are stored in separate files, provide a DataFrame to the `source` parameter.

    For more details and example usage, consult https://rasterframes.io/raster-read.html

    :param source: a string, list of strings, list of lists of strings, a Pandas DataFrame or a Spark DataFrame giving URIs to the raster data to read.
    :param catalog_col_names: required if `source` is a DataFrame or CSV string. It is a list of strings giving the names of columns containing URIs to read.
    :param band_indexes: list of integers indicating which bands, zero-based, to read from the raster files specified; default is to read only the first band.
    :param tile_dimensions: tuple or list of two indicating the default tile dimension as (columns, rows).
    :param buffer_size: buffer each tile read by this many cells on all sides.
    :param lazy_tiles: If true (default) only generate minimal references to tile contents; if false, fetch tile cell values.
    :param spatial_index_partitions: If true, partitions read tiles by a Z2 spatial index using the default shuffle partitioning.
           If a values > 0, the given number of partitions are created instead of the default.
    :param options: Additional keyword arguments to pass to the Spark DataSource.
    """

    from pandas import DataFrame as PdDataFrame

    if 'catalog' in options:
        source = options['catalog']  # maintain back compatibility with 0.8.0

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

    if spatial_index_partitions:
        num = int(spatial_index_partitions)
        if num < 0:
            spatial_index_partitions = '-1'
        elif num == 0:
            spatial_index_partitions = None

    if spatial_index_partitions:
        if spatial_index_partitions == True:
            spatial_index_partitions = "-1"
        else:
            spatial_index_partitions = str(spatial_index_partitions)
        options.update({"spatial_index_partitions": spatial_index_partitions})

    options.update({
        "band_indexes": to_csv(band_indexes),
        "tile_dimensions": to_csv(tile_dimensions),
        "lazy_tiles": str(lazy_tiles),
        "buffer_size": int(buffer_size)
    })

    # Parse the `source` argument
    path = None  # to pass into `path` param
    if isinstance(source, list):
        if all([isinstance(i, str) for i in source]):
            path = None
            catalog = None
            options.update(dict(paths='\n'.join([str(i) for i in source])))  # pass in "uri1\nuri2\nuri3\n..."
        if all([isinstance(i, list) for i in source]):
            # list of lists; we will rely on pandas to:
            #   - coerce all data to str (possibly using objects' __str__ or __repr__)
            #   - ensure data is not "ragged": all sublists are same len
            path = None
            catalog_col_names = ['proj_raster_{}'.format(i) for i in range(len(source[0]))]  # assign these names
            catalog = PdDataFrame(source,
                                  columns=catalog_col_names,
                                  dtype=str,
                                  )
    elif isinstance(source, str):
        if '\n' in source or '\r' in source:
            # then the `source` string is a catalog as a CSV (header is required)
            path = None
            catalog = source
        else:
            # interpret source as a single URI string
            path = source
            catalog = None
    else:
        # user has passed in some other type, we will try to interpret as a catalog
        catalog = source

    if catalog is not None:
        if catalog_col_names is None:
            raise Exception("'catalog_col_names' required when DataFrame 'catalog' specified")

        if isinstance(catalog, str):
            options.update({
                "catalog_csv": catalog,
                "catalog_col_names": to_csv(catalog_col_names)
            })
        elif isinstance(catalog, DataFrame):
            # check catalog_col_names
            assert all([c in catalog.columns for c in catalog_col_names]), \
                "All items in catalog_col_names must be the name of a column in the catalog DataFrame."
            # Create a random view name
            tmp_name = temp_name()
            catalog.createOrReplaceTempView(tmp_name)
            options.update({
                "catalog_table": tmp_name,
                "catalog_col_names": to_csv(catalog_col_names)
            })
        elif isinstance(catalog, PdDataFrame):
            # check catalog_col_names
            assert all([c in catalog.columns for c in catalog_col_names]), \
                "All items in catalog_col_names must be the name of a column in the catalog DataFrame."

            # Handle to active spark session
            session = SparkContext._active_spark_context._rf_context._spark_session
            # Create a random view name
            tmp_name = temp_name()
            spark_catalog = session.createDataFrame(catalog)
            spark_catalog.createOrReplaceTempView(tmp_name)
            options.update({
                "catalog_table": tmp_name,
                "catalog_col_names": to_csv(catalog_col_names)
            })

    return df_reader \
        .format("raster") \
        .load(path, **options)

def _stac_api_reader(
        df_reader: DataFrameReader,
        uri: str,
        filters: dict = None) -> DataFrame:
    """
    :param uri: STAC API uri
    :param filters: STAC API Search filters dict (bbox, datetime, intersects, collections, items, limit, query, next), see the STAC API Spec for more details https://github.com/radiantearth/stac-api-spec
    """
    import json

    return df_reader \
        .format("stac-api") \
        .option("uri", uri) \
        .option("search-filters", json.dumps(filters)) \
        .load()

def _geotiff_writer(
        df_writer: DataFrameWriter,
        path: str,
        crs: Optional[str] = None,
        raster_dimensions: Tuple[int] = None,
        **options: str):

    def set_dims(parts):
        parts = [int(p) for p in parts]
        assert len(parts) == 2, "Expected dimensions specification to have exactly two components"
        assert all([p > 0 for p in parts]), "Expected all components in dimensions to be positive integers"
        options.update({
            "imageWidth": str(parts[0]),
            "imageHeight": str(parts[1])
        })
        parts = [int(p) for p in parts]
        assert all([p > 0 for p in parts]), 'nice message'

    if raster_dimensions is not None:
        if isinstance(raster_dimensions, (list, tuple)):
            set_dims(raster_dimensions)
        elif isinstance(raster_dimensions, str):
            set_dims(raster_dimensions.split(','))

    if crs is not None:
        options.update({
            "crs": crs
        })

    return _aliased_writer(df_writer, "geotiff", path, **options)


# Patch RasterFrames initialization method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

# Patch Kryo serialization initialization method on SparkSession.Builder to mirror Scala approach
SparkSession.Builder.withKryoSerialization = _kryo_init

# Add the 'asLayer' method to pyspark DataFrame
DataFrame.as_layer = _convert_df

# Add `raster_join` method to pyspark DataFrame
DataFrame.raster_join = _raster_join

# Add DataSource convenience methods to the DataFrameReader
DataFrameReader.raster = _raster_reader
DataFrameReader.geojson = lambda df_reader, path: _aliased_reader(df_reader, "geojson", path)
DataFrameReader.geotiff = lambda df_reader, path: _layer_reader(df_reader, "geotiff", path)
DataFrameWriter.geotiff = _geotiff_writer
DataFrameReader.geotrellis = lambda df_reader, path: _layer_reader(df_reader, "geotrellis", path)
DataFrameReader.geotrellis_catalog = lambda df_reader, path: _aliased_reader(df_reader, "geotrellis-catalog", path)
DataFrameWriter.geotrellis = lambda df_writer, path: _aliased_writer(df_writer, "geotrellis", path)
DataFrameReader.stacapi = _stac_api_reader
