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
from pyrasterframes.rf_types import *
from pyspark.sql.functions import *
import pandas as pd
from shapely.geometry import Point
import os.path
from unittest import skip
from . import TestEnvironment


class RasterSourceTest(TestEnvironment):

    @staticmethod
    def path(scene, band):
        scene_dict = {
            1: 'https://landsat-pds.s3.amazonaws.com/c1/L8/015/041/LC08_L1TP_015041_20190305_20190309_01_T1/LC08_L1TP_015041_20190305_20190309_01_T1_B{}.TIF',
            2: 'https://landsat-pds.s3.amazonaws.com/c1/L8/015/042/LC08_L1TP_015042_20190305_20190309_01_T1/LC08_L1TP_015042_20190305_20190309_01_T1_B{}.TIF',
            3: 'https://landsat-pds.s3.amazonaws.com/c1/L8/016/041/LC08_L1TP_016041_20190224_20190309_01_T1/LC08_L1TP_016041_20190224_20190309_01_T1_B{}.TIF',
        }

        assert band in range(1, 12)
        assert scene in scene_dict.keys()
        p = scene_dict[scene]
        return p.format(band)

    def path_pandas_df(self):
        return pd.DataFrame([
            {'b1': self.path(1, 1), 'b2': self.path(1, 2), 'b3': self.path(1, 3), 'geo': Point(1, 1)},
            {'b1': self.path(2, 1), 'b2': self.path(2, 2), 'b3': self.path(2, 3), 'geo': Point(2, 2)},
            {'b1': self.path(3, 1), 'b2': self.path(3, 2), 'b3': self.path(3, 3), 'geo': Point(3, 3)},
        ])


    def test_handle_lazy_eval(self):
        df = self.spark.read.raster(self.path(1, 1))
        ltdf = df.select('proj_raster')
        self.assertGreater(ltdf.count(), 0)
        self.assertIsNotNone(ltdf.first().proj_raster)

        tdf = df.select(rf_tile('proj_raster').alias('pr'))
        self.assertGreater(tdf.count(),  0)
        self.assertIsNotNone(tdf.first().pr)

    def test_strict_eval(self):
        df_lazy = self.spark.read.raster(self.img_uri, lazy_tiles=True)
        # when doing Show on a lazy tile we will see something like RasterRefTile(RasterRef(JVMGeoTiffRasterSource(...
        # use this trick to get the `show` string
        show_str_lazy = df_lazy.select('proj_raster')._jdf.showString(1, -1, False)
        print(show_str_lazy)
        self.assertTrue('RasterRef' in show_str_lazy)

        # again for strict
        df_strict = self.spark.read.raster(self.img_uri, lazy_tiles=False)
        show_str_strict = df_strict.select('proj_raster')._jdf.showString(1, -1, False)
        self.assertTrue('RasterRef' not in show_str_strict)

    def test_prt_functions(self):
        df = self.spark.read.raster(self.img_uri) \
            .withColumn('crs', rf_crs('proj_raster')) \
            .withColumn('ext', rf_extent('proj_raster')) \
            .withColumn('geom', rf_geometry('proj_raster'))
        df.select('crs', 'ext', 'geom').first()

    def test_list_of_str(self):
        # much the same as RasterSourceDataSourceSpec here; but using https PDS. Takes about 30s to run

        def l8path(b):
            assert b in range(1, 12)
            base = "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/199/026/LC08_L1TP_199026_20180919_20180928_01_T1/LC08_L1TP_199026_20180919_20180928_01_T1_B{}.TIF"
            return base.format(b)

        path_param = [l8path(b) for b in [1, 2, 3]]
        tile_size = 512

        df = self.spark.read.raster(
            path_param,
            tile_dimensions=(tile_size, tile_size),
            lazy_tiles=True,
        ).cache()

        print(df.take(3))

        # schema is tile_path and tile
        # df.printSchema()
        self.assertTrue(len(df.columns) == 2 and 'proj_raster_path' in df.columns and 'proj_raster' in df.columns)

        # the most common tile dimensions should be as passed to `options`, showing that options are correctly applied
        tile_size_df = df.select(rf_dimensions(df.proj_raster).rows.alias('r'), rf_dimensions(df.proj_raster).cols.alias('c')) \
            .groupby(['r', 'c']).count().toPandas()
        most_common_size = tile_size_df.loc[tile_size_df['count'].idxmax()]
        self.assertTrue(most_common_size.r == tile_size and most_common_size.c == tile_size)

        # all rows are from a single source URI
        path_count = df.groupby(df.proj_raster_path).count()
        print(path_count.collect())
        self.assertTrue(path_count.count() == 3)

    def test_list_of_list_of_str(self):
        lol = [
            [self.path(1, 1), self.path(1, 2)],
            [self.path(2, 1), self.path(2, 2)],
            [self.path(3, 1), self.path(3, 2)]
        ]
        df = self.spark.read.raster(lol)
        self.assertTrue(len(df.columns) == 4)  # 2 cols of uris plus 2 cols of proj_rasters
        self.assertEqual(sorted(df.columns), sorted(['proj_raster_0_path', 'proj_raster_1_path',
                                                     'proj_raster_0', 'proj_raster_1']))
        uri_df = df.select('proj_raster_0_path', 'proj_raster_1_path').distinct()
        
        # check that various uri's are in the dataframe
        self.assertEqual(
            uri_df.filter(col('proj_raster_0_path') == lit(self.path(1, 1))).count(),
            1)
        
        self.assertEqual(
            uri_df \
                .filter(col('proj_raster_0_path') == lit(self.path(1, 1))) \
                .filter(col('proj_raster_1_path') == lit(self.path(1, 2))) \
                .count(),
            1)
        
        self.assertEqual(
            uri_df \
                .filter(col('proj_raster_0_path') == lit(self.path(3, 1))) \
                .filter(col('proj_raster_1_path') == lit(self.path(3, 2))) \
                .count(),
            1)

    def test_schemeless_string(self):
        import os.path
        path = os.path.join(self.resource_dir, "L8-B8-Robinson-IL.tiff")
        self.assertTrue(not path.startswith('file://'))
        self.assertTrue(os.path.exists(path))
        df = self.spark.read.raster(path)
        self.assertTrue(df.count() > 0)

    def test_spark_df_source(self):
        catalog_columns = ['b1', 'b2', 'b3']
        catalog = self.spark.createDataFrame(self.path_pandas_df())

        df = self.spark.read.raster(
            catalog,
            tile_dimensions=(512, 512),
            catalog_col_names=catalog_columns,
            lazy_tiles=True  # We'll get an OOM error if we try to read 9 scenes all at once!
        )

        self.assertTrue(len(df.columns) == 7)  # three bands times {path, tile} plus geo
        self.assertTrue(df.select('b1_path').distinct().count() == 3)  # as per scene_dict
        b1_paths_maybe = df.select('b1_path').distinct().collect()
        b1_paths = [self.path(s, 1) for s in [1, 2, 3]]
        self.assertTrue(all([row.b1_path in b1_paths for row in b1_paths_maybe]))

    def test_pandas_source(self):

        df = self.spark.read.raster(
            self.path_pandas_df(),
            catalog_col_names=['b1', 'b2', 'b3']
        )
        self.assertEqual(len(df.columns), 7)  # three path cols, three tile cols, and geo
        self.assertTrue('geo' in df.columns)
        self.assertTrue(df.select('b1_path').distinct().count() == 3)

    def test_geopandas_source(self):
        from geopandas import GeoDataFrame
        # Same test as test_pandas_source with geopandas
        geo_df = GeoDataFrame(self.path_pandas_df(), crs={'init': 'EPSG:4326'}, geometry='geo')
        df = self.spark.read.raster(geo_df, ['b1', 'b2', 'b3'])

        self.assertEqual(len(df.columns), 7)  # three path cols, three tile cols, and geo
        self.assertTrue('geo' in df.columns)
        self.assertTrue(df.select('b1_path').distinct().count() == 3)

    def test_csv_string(self):

        s = """metadata,b1,b2
        a,{},{}
        b,{},{}
        c,{},{}
        """.format(
            self.path(1, 1), self.path(1, 2),
            self.path(2, 1), self.path(2, 2),
            self.path(3, 1), self.path(3, 2),
                   )

        df = self.spark.read.raster(s, ['b1', 'b2'])
        self.assertEqual(len(df.columns), 3 + 2)  # number of columns in original DF plus cardinality of catalog_col_names
        self.assertTrue(len(df.take(1)))  # non-empty check

    def test_catalog_named_arg(self):
        # through version 0.8.1 reading a catalog was via named argument only.
        df = self.spark.read.raster(catalog=self.path_pandas_df(), catalog_col_names=['b1', 'b2', 'b3'])
        self.assertEqual(len(df.columns), 7)  # three path cols, three tile cols, and geo
        self.assertTrue(df.select('b1_path').distinct().count() == 3)

    def test_spatial_partitioning(self):
        f = self.path(1, 1)
        df = self.spark.read.raster(f, spatial_index_partitions=True)
        self.assertTrue('spatial_index' in df.columns)

        self.assertEqual(df.rdd.getNumPartitions(), int(self.spark.conf.get("spark.sql.shuffle.partitions")))
        self.assertEqual(self.spark.read.raster(f, spatial_index_partitions=34).rdd.getNumPartitions(), 34)
        self.assertEqual(self.spark.read.raster(f, spatial_index_partitions="42").rdd.getNumPartitions(), 42)
        self.assertFalse('spatial_index' in self.spark.read.raster(f, spatial_index_partitions=False).columns)
        self.assertFalse('spatial_index' in self.spark.read.raster(f, spatial_index_partitions=0).columns)