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

from pyrasterframes.rasterfunctions import *
from pyspark.sql import Row
from pyspark.sql.functions import *

from . import TestEnvironment


class VectorTypes(TestEnvironment):

    def setUp(self):
        self.create_layer()
        import pandas as pd
        self.pandas_df = pd.DataFrame({
            'eye': ['a', 'b', 'c', 'd'],
            'x': [0.0, 1.0, 2.0, 3.0],
            'y': [-4.0, -3.0, -2.0, -1.0],
        })
        df = self.spark.createDataFrame(self.pandas_df)
        df = df.withColumn("point_geom",
                           st_point(df.x, df.y)
                           )
        self.df = df.withColumn("poly_geom", st_bufferPoint(df.point_geom, lit(1250.0)))

    def test_spatial_relations(self):
        from pyspark.sql.functions import udf, sum
        from geomesa_pyspark.types import PointUDT
        import shapely
        import numpy.testing

        # Use python shapely UDT in a UDF
        @udf("double")
        def area_fn(g):
            return g.area

        @udf("double")
        def length_fn(g):
            return g.length

        df = self.df.withColumn("poly_area", area_fn(self.df.poly_geom))
        df = df.withColumn("poly_len", length_fn(df.poly_geom))

        # Return UDT in a UDF!
        def some_point(g):
            return g.representative_point()

        some_point_udf = udf(some_point, PointUDT())

        df = df.withColumn("any_point", some_point_udf(df.poly_geom))
        # spark-side UDF/UDT are correct
        intersect_total = df.agg(sum(
            st_intersects(df.poly_geom, df.any_point).astype('double')
        ).alias('s')).collect()[0].s
        self.assertTrue(intersect_total == df.count())

        # Collect to python driver in shapely UDT
        pandas_df_out = df.toPandas()

        # Confirm we get a shapely type back from st_* function and UDF
        self.assertIsInstance(pandas_df_out.poly_geom.iloc[0], shapely.geometry.Polygon)
        self.assertIsInstance(pandas_df_out.any_point.iloc[0], shapely.geometry.Point)

        # And our spark-side manipulations were correct
        xs_correct = pandas_df_out.point_geom.apply(lambda g: g.coords[0][0]) == self.pandas_df.x
        self.assertTrue(all(xs_correct))

        centroid_ys = pandas_df_out.poly_geom.apply(lambda g:
                                                    g.centroid.coords[0][1]).tolist()
        numpy.testing.assert_almost_equal(centroid_ys, self.pandas_df.y.tolist())

        # Including from UDF's
        numpy.testing.assert_almost_equal(
            pandas_df_out.poly_geom.apply(lambda g: g.area).values,
            pandas_df_out.poly_area.values
        )
        numpy.testing.assert_almost_equal(
            pandas_df_out.poly_geom.apply(lambda g: g.length).values,
            pandas_df_out.poly_len.values
        )

    def test_geometry_udf(self):
        from geomesa_pyspark.types import PolygonUDT
        # simple test that raster contents are not invalid

        # create a udf to buffer (the bounds) polygon
        def _buffer(g, d):
            return g.buffer(d)

        @udf("double")
        def area(g):
            return g.area

        buffer_udf = udf(_buffer, PolygonUDT())

        buf_cells = 10
        with_poly = self.rf.withColumn('poly', buffer_udf(self.rf.geometry, lit(-15 * buf_cells)))  # cell res is 15x15
        area = with_poly.select(area('poly') < area('geometry'))
        area_result = area.collect()
        self.assertTrue(all([r[0] for r in area_result]))

    def test_rasterize(self):
        from geomesa_pyspark.types import PolygonUDT

        @udf(PolygonUDT())
        def buffer(g, d):
            return g.buffer(d)

        # start with known polygon, the tile extents, **negative buffered**  by 10 cells
        buf_cells = 10
        with_poly = self.rf.withColumn('poly', buffer(self.rf.geometry, lit(-15 * buf_cells)))  # cell res is 15x15

        # rasterize value 16 into buffer shape.
        cols = 194  # from dims of tile
        rows = 250  # from dims of tile
        with_raster = with_poly.withColumn('rasterized',
                                           rf_rasterize('poly', 'geometry', lit(16), lit(cols), lit(rows)))
        result = with_raster.select(rf_tile_sum(rf_local_equal_int(with_raster.rasterized, 16)),
                                    rf_tile_sum(with_raster.rasterized))
        #
        expected_burned_in_cells = (cols - 2 * buf_cells) * (rows - 2 * buf_cells)
        self.assertEqual(result.first()[0], float(expected_burned_in_cells))
        self.assertEqual(result.first()[1], 16. * expected_burned_in_cells)

    def test_parse_crs(self):
        df = self.spark.createDataFrame([Row(id=1)])
        self.assertEqual(df.select(rf_mk_crs('EPSG:4326')).count(), 1)

    def test_reproject(self):
        reprojected = self.rf.withColumn('reprojected',
                                         st_reproject('center', rf_mk_crs('EPSG:4326'), rf_mk_crs('EPSG:3857')))
        reprojected.show()
        self.assertEqual(reprojected.count(), 8)

    def test_geojson(self):
        import os
        sample = 'file://' + os.path.join(self.resource_dir, 'buildings.geojson')
        geo = self.spark.read.geojson(sample)
        geo.show()
        self.assertEqual(geo.select('geometry').count(), 8)

    def test_xz2_index(self):
        from pyspark.sql.functions import min as F_min
        df = self.df.select(rf_xz2_index(self.df.poly_geom, rf_crs(lit("EPSG:4326"))).alias('index'))
        expected = {22858201775, 38132946267, 38166922588, 38180072113}
        indexes = {x[0] for x in df.collect()}
        self.assertSetEqual(indexes, expected)

        # Test against proj_raster (has CRS and Extent embedded).
        result_one_arg = self.df.select(rf_xz2_index('tile').alias('ix')) \
            .agg(F_min('ix')).first()[0]

        result_two_arg = self.df.select(rf_xz2_index(rf_extent('tile'), rf_crs('tile')).alias('ix')) \
            .agg(F_min('ix')).first()[0]

        self.assertEqual(result_two_arg, result_one_arg)
        self.assertEqual(result_one_arg, 55179438768)  # this is a bit more fragile but less important

        # Custom resolution
        df = self.df.select(rf_xz2_index(self.df.poly_geom, rf_crs(lit("EPSG:4326")), 3).alias('index'))
        expected = {21, 36}
        indexes = {x[0] for x in df.collect()}
        self.assertSetEqual(indexes, expected)

    def test_z2_index(self):
        df = self.df.select(rf_z2_index(self.df.poly_geom, rf_crs(lit("EPSG:4326"))).alias('index'))

        expected = {28596898472, 28625192874, 28635062506, 28599712232}
        indexes = {x[0] for x in df.collect()}
        self.assertSetEqual(indexes, expected)

        # Custom resolution
        df = self.df.select(rf_z2_index(self.df.poly_geom, rf_crs(lit("EPSG:4326")), 6).alias('index'))
        expected = {1704, 1706}
        indexes = {x[0] for x in df.collect()}
        self.assertSetEqual(indexes, expected)
