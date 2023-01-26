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

import os

import numpy.testing
import pandas as pd
import pyspark.sql.functions as F
import pytest
import shapely
from geomesa_pyspark.types import PointUDT, PolygonUDT
from pyrasterframes.rasterfunctions import *
from pyspark.sql import Row


@pytest.fixture
def pandas_df():
    return pd.DataFrame(
        {"eye": ["a", "b", "c", "d"], "x": [0.0, 1.0, 2.0, 3.0], "y": [-4.0, -3.0, -2.0, -1.0],}
    )


@pytest.fixture
def df(spark, pandas_df):

    df = spark.createDataFrame(pandas_df)
    df = df.withColumn("point_geom", st_point(df.x, df.y))
    return df.withColumn("poly_geom", st_bufferPoint(df.point_geom, lit(1250.0)))


def test_spatial_relations(df, pandas_df):

    # Use python shapely UDT in a UDF
    @F.udf("double")
    def area_fn(g):
        return g.area

    @F.udf("double")
    def length_fn(g):
        return g.length

    df = df.withColumn("poly_area", area_fn(df.poly_geom))
    df = df.withColumn("poly_len", length_fn(df.poly_geom))

    # Return UDT in a UDF!
    def some_point(g):
        return g.representative_point()

    some_point_udf = F.udf(some_point, PointUDT())

    df = df.withColumn("any_point", some_point_udf(df.poly_geom))
    # spark-side UDF/UDT are correct
    intersect_total = (
        df.agg(F.sum(st_intersects(df.poly_geom, df.any_point).astype("double")).alias("s"))
        .collect()[0]
        .s
    )
    assert intersect_total == df.count()

    # Collect to python driver in shapely UDT
    pandas_df_out = df.toPandas()

    # Confirm we get a shapely type back from st_* function and UDF
    assert isinstance(pandas_df_out.poly_geom.iloc[0], shapely.geometry.Polygon)
    assert isinstance(pandas_df_out.any_point.iloc[0], shapely.geometry.Point)

    # And our spark-side manipulations were correct
    xs_correct = pandas_df_out.point_geom.apply(lambda g: g.coords[0][0]) == pandas_df.x
    assert all(xs_correct)

    centroid_ys = pandas_df_out.poly_geom.apply(lambda g: g.centroid.coords[0][1]).tolist()
    numpy.testing.assert_almost_equal(centroid_ys, pandas_df.y.tolist())

    # Including from UDF's
    numpy.testing.assert_almost_equal(
        pandas_df_out.poly_geom.apply(lambda g: g.area).values, pandas_df_out.poly_area.values
    )
    numpy.testing.assert_almost_equal(
        pandas_df_out.poly_geom.apply(lambda g: g.length).values, pandas_df_out.poly_len.values
    )


def test_geometry_udf(rf):

    # simple test that raster contents are not invalid
    # create a udf to buffer (the bounds) polygon
    def _buffer(g, d):
        return g.buffer(d)

    @F.udf("double")
    def area(g):
        return g.area

    buffer_udf = F.udf(_buffer, PolygonUDT())

    buf_cells = 10
    with_poly = rf.withColumn(
        "poly", buffer_udf(rf.geometry, F.lit(-15 * buf_cells))
    )  # cell res is 15x15
    area = with_poly.select(area("poly") < area("geometry"))
    area_result = area.collect()
    assert all([r[0] for r in area_result])


def test_rasterize(rf):
    @F.udf(PolygonUDT())
    def buffer(g, d):
        return g.buffer(d)

    # start with known polygon, the tile extents, **negative buffered**  by 10 cells
    buf_cells = 10
    with_poly = rf.withColumn(
        "poly", buffer(rf.geometry, lit(-15 * buf_cells))
    )  # cell res is 15x15

    # rasterize value 16 into buffer shape.
    cols = 194  # from dims of tile
    rows = 250  # from dims of tile
    with_raster = with_poly.withColumn(
        "rasterized", rf_rasterize("poly", "geometry", lit(16), lit(cols), lit(rows))
    )
    result = with_raster.select(
        rf_tile_sum(rf_local_equal_int(with_raster.rasterized, 16)),
        rf_tile_sum(with_raster.rasterized),
    )
    #
    expected_burned_in_cells = (cols - 2 * buf_cells) * (rows - 2 * buf_cells)
    assert result.first()[0] == float(expected_burned_in_cells)
    assert result.first()[1] == 16.0 * expected_burned_in_cells


def test_parse_crs(spark):
    df = spark.createDataFrame([Row(id=1)])
    assert df.select(rf_mk_crs("EPSG:4326")).count() == 1


def test_reproject(rf):
    reprojected = rf.withColumn(
        "reprojected", st_reproject("center", rf_mk_crs("EPSG:4326"), rf_mk_crs("EPSG:3857"))
    )
    reprojected.show()
    assert reprojected.count() == 8


def test_geojson(spark, resource_dir):

    sample = "file://" + os.path.join(resource_dir, "buildings.geojson")
    geo = spark.read.geojson(sample)
    geo.show()
    assert geo.select("geometry").count() == 8


def test_xz2_index(spark, img_uri, df):

    df1 = df.select(rf_xz2_index(df.poly_geom, rf_crs(F.lit("EPSG:4326"))).alias("index"))
    expected = {22858201775, 38132946267, 38166922588, 38180072113}
    indexes = {x[0] for x in df1.collect()}
    assert indexes == expected

    # Test against proj_raster (has CRS and Extent embedded).
    df2 = spark.read.raster(img_uri)
    result_one_arg = df2.select(rf_xz2_index("proj_raster").alias("ix")).agg(F.min("ix")).first()[0]

    result_two_arg = (
        df2.select(rf_xz2_index(rf_extent("proj_raster"), rf_crs("proj_raster")).alias("ix"))
        .agg(F.min("ix"))
        .first()[0]
    )

    assert result_two_arg == result_one_arg
    assert result_one_arg == 55179438768  # this is a bit more fragile but less important

    # Custom resolution
    df3 = df.select(rf_xz2_index(df.poly_geom, rf_crs(lit("EPSG:4326")), 3).alias("index"))
    expected = {21, 36}
    indexes = {x[0] for x in df3.collect()}
    assert indexes == expected


def test_z2_index(df):
    df1 = df.select(rf_z2_index(df.poly_geom, rf_crs(lit("EPSG:4326"))).alias("index"))

    expected = {28596898472, 28625192874, 28635062506, 28599712232}
    indexes = {x[0] for x in df1.collect()}
    assert indexes == expected

    # Custom resolution
    df2 = df.select(rf_z2_index(df.poly_geom, rf_crs(lit("EPSG:4326")), 6).alias("index"))
    expected = {1704, 1706}
    indexes = {x[0] for x in df2.collect()}
    assert indexes == expected


def test_agg_extent(df):
    r = (
        df.select(rf_agg_extent(st_extent("poly_geom")).alias("agg_extent"))
        .select("agg_extent.*")
        .first()
    )
    assert (
        r.asDict()
        == Row(
            xmin=-0.011268955205879273,
            ymin=-4.011268955205879,
            xmax=3.0112432169934484,
            ymax=-0.9887567830065516,
        ).asDict()
    )


def test_agg_reprojected_extent(df):
    r = df.select(
        rf_agg_reprojected_extent(st_extent("poly_geom"), rf_mk_crs("EPSG:4326"), "EPSG:3857")
    ).first()[0]
    assert (
        r.asDict()
        == Row(
            xmin=-1254.45435529069,
            ymin=-446897.63591665257,
            xmax=335210.0615704097,
            ymax=-110073.36515944061,
        ).asDict()
    )
