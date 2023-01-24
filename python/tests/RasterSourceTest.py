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

import json
import os.path
import urllib.request
from functools import cache
from unittest import skip

import pandas as pd
import pyspark.sql.functions as F
from geopandas import GeoDataFrame
from shapely.geometry import Point

from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *


@cache
def get_signed_url(url):
    sas_url = f"https://planetarycomputer.microsoft.com/api/sas/v1/sign?href={url}"
    with urllib.request.urlopen(sas_url) as response:
        signed_url = json.loads(response.read())["href"]
    return signed_url


def path(scene, band):

    scene_dict = {
        1: "https://landsateuwest.blob.core.windows.net/landsat-c2/level-2/standard/oli-tirs/2022/195/023/LC08_L2SP_195023_20220902_20220910_02_T1/LC08_L2SP_195023_20220902_20220910_02_T1_SR_B{}.TIF",
        2: "https://landsateuwest.blob.core.windows.net/landsat-c2/level-2/standard/oli-tirs/2022/195/022/LC08_L2SP_195022_20220902_20220910_02_T1/LC08_L2SP_195022_20220902_20220910_02_T1_SR_B{}.TIF",
        3: "https://landsateuwest.blob.core.windows.net/landsat-c2/level-2/standard/oli-tirs/2022/196/022/LC08_L2SP_196022_20220418_20220427_02_T1/LC08_L2SP_196022_20220418_20220427_02_T1_SR_B{}.TIF",
    }

    assert band in range(1, 12)
    assert scene in scene_dict.keys()
    p = scene_dict[scene]
    return get_signed_url(p.format(band))


def path_pandas_df():
    return pd.DataFrame(
        [
            {"b1": path(1, 1), "b2": path(1, 2), "b3": path(1, 3), "geo": Point(1, 1),},
            {"b1": path(2, 1), "b2": path(2, 2), "b3": path(2, 3), "geo": Point(2, 2),},
            {"b1": path(3, 1), "b2": path(3, 2), "b3": path(3, 3), "geo": Point(3, 3),},
        ]
    )


def test_handle_lazy_eval(spark):
    df = spark.read.raster(path(1, 1))
    ltdf = df.select("proj_raster")
    assert ltdf.count() > 0
    assert ltdf.first().proj_raster is not None

    tdf = df.select(rf_tile("proj_raster").alias("pr"))
    assert tdf.count() > 0
    assert tdf.first().pr is not None


def test_strict_eval(spark, img_uri):
    df_lazy = spark.read.raster(img_uri, lazy_tiles=True)
    # when doing Show on a lazy tile we will see something like RasterRefTile(RasterRef(JVMGeoTiffRasterSource(...
    # use this trick to get the `show` string
    show_str_lazy = df_lazy.select("proj_raster")._jdf.showString(1, -1, False)
    print(show_str_lazy)
    assert "RasterRef" in show_str_lazy

    # again for strict
    df_strict = spark.read.raster(img_uri, lazy_tiles=False)
    show_str_strict = df_strict.select("proj_raster")._jdf.showString(1, -1, False)
    assert "RasterRef" not in show_str_strict


def test_prt_functions(spark, img_uri):
    df = (
        spark.read.raster(img_uri)
        .withColumn("crs", rf_crs("proj_raster"))
        .withColumn("ext", rf_extent("proj_raster"))
        .withColumn("geom", rf_geometry("proj_raster"))
    )
    df.select("crs", "ext", "geom").first()


def test_list_of_str(spark):
    # much the same as RasterSourceDataSourceSpec here; but using https PDS. Takes about 30s to run

    def l8path(b):
        assert b in range(1, 12)

        base = "https://landsateuwest.blob.core.windows.net/landsat-c2/level-2/standard/oli-tirs/2022/196/022/LC08_L2SP_196022_20220418_20220427_02_T1/LC08_L2SP_196022_20220418_20220427_02_T1_SR_B{}.TIF"
        return get_signed_url(base.format(b))

    path_param = [l8path(b) for b in [1, 2, 3]]
    tile_size = 512

    df = spark.read.raster(
        path_param, tile_dimensions=(tile_size, tile_size), lazy_tiles=True,
    ).cache()

    print(df.take(3))

    # schema is tile_path and tile
    # df.printSchema()
    assert len(df.columns) == 2 and "proj_raster_path" in df.columns and "proj_raster" in df.columns

    # the most common tile dimensions should be as passed to `options`, showing that options are correctly applied
    tile_size_df = (
        df.select(
            rf_dimensions(df.proj_raster).rows.alias("r"),
            rf_dimensions(df.proj_raster).cols.alias("c"),
        )
        .groupby(["r", "c"])
        .count()
        .toPandas()
    )
    most_common_size = tile_size_df.loc[tile_size_df["count"].idxmax()]
    assert most_common_size.r == tile_size and most_common_size.c == tile_size

    # all rows are from a single source URI
    path_count = df.groupby(df.proj_raster_path).count()
    print(path_count.collect())
    assert path_count.count() == 3


def test_list_of_list_of_str(spark):
    lol = [
        [path(1, 1), path(1, 2)],
        [path(2, 1), path(2, 2)],
        [path(3, 1), path(3, 2)],
    ]
    df = spark.read.raster(lol)
    assert len(df.columns) == 4  # 2 cols of uris plus 2 cols of proj_rasters
    assert sorted(df.columns) == sorted(
        ["proj_raster_0_path", "proj_raster_1_path", "proj_raster_0", "proj_raster_1"]
    )
    uri_df = df.select("proj_raster_0_path", "proj_raster_1_path").distinct()

    # check that various uri's are in the dataframe
    assert uri_df.filter(F.col("proj_raster_0_path") == F.lit(path(1, 1))).count() == 1

    assert (
        uri_df.filter(F.col("proj_raster_0_path") == F.lit(path(1, 1)))
        .filter(F.col("proj_raster_1_path") == F.lit(path(1, 2)))
        .count()
        == 1
    )

    assert (
        uri_df.filter(F.col("proj_raster_0_path") == F.lit(path(3, 1)))
        .filter(F.col("proj_raster_1_path") == F.lit(path(3, 2)))
        .count()
        == 1
    )


def test_schemeless_string(spark, resource_dir):

    path = os.path.join(resource_dir, "L8-B8-Robinson-IL.tiff")
    assert not path.startswith("file://")
    assert os.path.exists(path)
    df = spark.read.raster(path)
    assert df.count() > 0


def test_spark_df_source(spark):
    catalog_columns = ["b1", "b2", "b3"]
    catalog = spark.createDataFrame(path_pandas_df())

    df = spark.read.raster(
        catalog,
        tile_dimensions=(512, 512),
        catalog_col_names=catalog_columns,
        lazy_tiles=True,  # We'll get an OOM error if we try to read 9 scenes all at once!
    )

    assert len(df.columns) == 7  # three bands times {path, tile} plus geo
    assert df.select("b1_path").distinct().count() == 3  # as per scene_dict
    b1_paths_maybe = df.select("b1_path").distinct().collect()
    b1_paths = [path(s, 1) for s in [1, 2, 3]]
    assert all([row.b1_path in b1_paths for row in b1_paths_maybe])


def test_pandas_source(spark):

    df = spark.read.raster(path_pandas_df(), catalog_col_names=["b1", "b2", "b3"])
    assert len(df.columns) == 7  # three path cols, three tile cols, and geo
    assert "geo" in df.columns
    assert df.select("b1_path").distinct().count() == 3


def test_geopandas_source(spark):

    # Same test as test_pandas_source with geopandas
    geo_df = GeoDataFrame(path_pandas_df(), crs={"init": "EPSG:4326"}, geometry="geo")
    df = spark.read.raster(geo_df, ["b1", "b2", "b3"])

    assert len(df.columns) == 7  # three path cols, three tile cols, and geo
    assert "geo" in df.columns
    assert df.select("b1_path").distinct().count() == 3


def test_csv_string(spark):

    s = """metadata,b1,b2
        a,{},{}
        b,{},{}
        c,{},{}
        """.format(
        path(1, 1), path(1, 2), path(2, 1), path(2, 2), path(3, 1), path(3, 2),
    )

    df = spark.read.raster(s, ["b1", "b2"])
    assert (
        len(df.columns) == 3 + 2
    )  # number of columns in original DF plus cardinality of catalog_col_names
    assert len(df.take(1))  # non-empty check


def test_catalog_named_arg(spark):
    # through version 0.8.1 reading a catalog was via named argument only.
    df = spark.read.raster(catalog=path_pandas_df(), catalog_col_names=["b1", "b2", "b3"])
    assert len(df.columns) == 7  # three path cols, three tile cols, and geo
    assert df.select("b1_path").distinct().count() == 3


def test_spatial_partitioning(spark):
    f = path(1, 1)
    df = spark.read.raster(f, spatial_index_partitions=True)
    assert "spatial_index" in df.columns

    assert df.rdd.getNumPartitions() == int(spark.conf.get("spark.sql.shuffle.partitions"))
    assert spark.read.raster(f, spatial_index_partitions=34).rdd.getNumPartitions() == 34
    assert spark.read.raster(f, spatial_index_partitions="42").rdd.getNumPartitions() == 42
    assert "spatial_index" not in spark.read.raster(f, spatial_index_partitions=False).columns
    assert "spatial_index" not in spark.read.raster(f, spatial_index_partitions=0).columns
