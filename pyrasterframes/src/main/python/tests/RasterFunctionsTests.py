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
from pyrasterframes.utils import gdal_version
from pyspark import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

from . import TestEnvironment


class RasterFunctions(TestEnvironment):

    def setUp(self):
        self.create_layer()

    def test_setup(self):
        self.assertEqual(self.spark.sparkContext.getConf().get("spark.serializer"),
                         "org.apache.spark.serializer.KryoSerializer")
        print("GDAL version", gdal_version())

    def test_identify_columns(self):
        cols = self.rf.tile_columns()
        self.assertEqual(len(cols), 1, '`tileColumns` did not find the proper number of columns.')
        print("Tile columns: ", cols)
        col = self.rf.spatial_key_column()
        self.assertIsInstance(col, Column, '`spatialKeyColumn` was not found')
        print("Spatial key column: ", col)
        col = self.rf.temporal_key_column()
        self.assertIsNone(col, '`temporalKeyColumn` should be `None`')
        print("Temporal key column: ", col)

    def test_tile_creation(self):
        from pyrasterframes.rf_types import CellType

        base = self.spark.createDataFrame([1, 2, 3, 4], 'integer')
        tiles = base.select(rf_make_constant_tile(3, 3, 3, "int32"), rf_make_zeros_tile(3, 3, "int32"),
                            rf_make_ones_tile(3, 3, CellType.int32()))
        tiles.show()
        self.assertEqual(tiles.count(), 4)

    def test_multi_column_operations(self):
        df1 = self.rf.withColumnRenamed('tile', 't1').as_layer()
        df2 = self.rf.withColumnRenamed('tile', 't2').as_layer()
        df3 = df1.spatial_join(df2).as_layer()
        df3 = df3.withColumn('norm_diff', rf_normalized_difference('t1', 't2'))
        # df3.printSchema()

        aggs = df3.agg(
            rf_agg_mean('norm_diff'),
        )
        aggs.show()
        row = aggs.first()

        self.assertTrue(self.rounded_compare(row['rf_agg_mean(norm_diff)'], 0))

    def test_general(self):
        meta = self.rf.tile_layer_metadata()
        self.assertIsNotNone(meta['bounds'])
        df = self.rf.withColumn('dims', rf_dimensions('tile')) \
            .withColumn('type', rf_cell_type('tile')) \
            .withColumn('dCells', rf_data_cells('tile')) \
            .withColumn('ndCells', rf_no_data_cells('tile')) \
            .withColumn('min', rf_tile_min('tile')) \
            .withColumn('max', rf_tile_max('tile')) \
            .withColumn('mean', rf_tile_mean('tile')) \
            .withColumn('sum', rf_tile_sum('tile')) \
            .withColumn('stats', rf_tile_stats('tile')) \
            .withColumn('extent', st_extent('geometry')) \
            .withColumn('extent_geom1', st_geometry('extent')) \
            .withColumn('ascii', rf_render_ascii('tile')) \
            .withColumn('log', rf_log('tile')) \
            .withColumn('exp', rf_exp('tile')) \
            .withColumn('expm1', rf_expm1('tile')) \
            .withColumn('round', rf_round('tile')) \
            .withColumn('abs', rf_abs('tile'))

        df.first()

    def test_agg_mean(self):
        mean = self.rf.agg(rf_agg_mean('tile')).first()['rf_agg_mean(tile)']
        self.assertTrue(self.rounded_compare(mean, 10160))

    def test_agg_local_mean(self):
        from pyspark.sql import Row
        from pyrasterframes.rf_types import Tile
        import numpy as np

        # this is really testing the nodata propagation in the agg  local summation
        ct = CellType.int8().with_no_data_value(4)
        df = self.spark.createDataFrame([
            Row(tile=Tile(np.array([[1, 2, 3, 4, 5, 6]]), ct)),
            Row(tile=Tile(np.array([[1, 2, 4, 3, 5, 6]]), ct)),
        ])

        result = df.agg(rf_agg_local_mean('tile').alias('mean')).first().mean

        expected = Tile(np.array([[1.0, 2.0, 3.0, 3.0, 5.0, 6.0]]), CellType.float64())
        self.assertEqual(result, expected)

    def test_aggregations(self):
        aggs = self.rf.agg(
            rf_agg_data_cells('tile'),
            rf_agg_no_data_cells('tile'),
            rf_agg_stats('tile'),
            rf_agg_approx_histogram('tile')
        )
        row = aggs.first()

        # print(row['rf_agg_data_cells(tile)'])
        self.assertEqual(row['rf_agg_data_cells(tile)'], 387000)
        self.assertEqual(row['rf_agg_no_data_cells(tile)'], 1000)
        self.assertEqual(row['rf_agg_stats(tile)'].data_cells, row['rf_agg_data_cells(tile)'])

    def test_sql(self):

        self.rf.createOrReplaceTempView("rf_test_sql")

        arith = self.spark.sql("""SELECT tile, 
                                rf_local_add(tile, 1) AS add_one, 
                                rf_local_subtract(tile, 1) AS less_one, 
                                rf_local_multiply(tile, 2) AS times_two, 
                                rf_local_divide(
                                    rf_convert_cell_type(tile, "float32"), 
                                    2) AS over_two 
                            FROM rf_test_sql""")

        arith.createOrReplaceTempView('rf_test_sql_1')
        arith.show(truncate=False)
        stats = self.spark.sql("""
            SELECT rf_tile_mean(tile) as base,
                rf_tile_mean(add_one) as plus_one,
                rf_tile_mean(less_one) as minus_one,
                rf_tile_mean(times_two) as double,
                rf_tile_mean(over_two) as half,
                rf_no_data_cells(tile) as nd
                
            FROM rf_test_sql_1
            ORDER BY rf_no_data_cells(tile)
            """)
        stats.show(truncate=False)
        stats.createOrReplaceTempView('rf_test_sql_stats')

        compare = self.spark.sql("""
            SELECT 
                plus_one - 1.0 = base as add,
                minus_one + 1.0 = base as subtract,
                double / 2.0 = base as multiply,
                half * 2.0 = base as divide,
                nd
            FROM rf_test_sql_stats
            """)

        expect_row1 = compare.orderBy('nd').first()

        self.assertTrue(expect_row1.subtract)
        self.assertTrue(expect_row1.multiply)
        self.assertTrue(expect_row1.divide)
        self.assertEqual(expect_row1.nd, 0)
        self.assertTrue(expect_row1.add)

        expect_row2 = compare.orderBy('nd', ascending=False).first()

        self.assertTrue(expect_row2.subtract)
        self.assertTrue(expect_row2.multiply)
        self.assertTrue(expect_row2.divide)
        self.assertTrue(expect_row2.nd > 0)
        self.assertTrue(expect_row2.add)  # <-- Would fail in a case where ND + 1 = 1

    def test_explode(self):
        import pyspark.sql.functions as F
        self.rf.select('spatial_key', rf_explode_tiles('tile')).show()
        # +-----------+------------+---------+-------+
        # |spatial_key|column_index|row_index|tile   |
        # +-----------+------------+---------+-------+
        # |[2,1]      |4           |0        |10150.0|
        cell = self.rf.select(self.rf.spatial_key_column(), rf_explode_tiles(self.rf.tile)) \
            .where(F.col("spatial_key.col") == 2) \
            .where(F.col("spatial_key.row") == 1) \
            .where(F.col("column_index") == 4) \
            .where(F.col("row_index") == 0) \
            .select(F.col("tile")) \
            .collect()[0][0]
        self.assertEqual(cell, 10150.0)

        # Test the sample version
        frac = 0.01
        sample_count = self.rf.select(rf_explode_tiles_sample(frac, 1872, 'tile')).count()
        print('Sample count is {}'.format(sample_count))
        self.assertTrue(sample_count > 0)
        self.assertTrue(sample_count < (frac * 1.1) * 387000)  # give some wiggle room

    def test_mask_by_value(self):
        from pyspark.sql.functions import lit

        # create an artificial mask for values > 25000; masking value will be 4
        mask_value = 4

        rf1 = self.rf.select(self.rf.tile,
                             rf_local_multiply(
                                 rf_convert_cell_type(
                                     rf_local_greater_int(self.rf.tile, 25000),
                                     "uint8"),
                                 lit(mask_value)).alias('mask'))
        rf2 = rf1.select(rf1.tile, rf_mask_by_value(rf1.tile, rf1.mask, lit(mask_value)).alias('masked'))
        result = rf2.agg(rf_agg_no_data_cells(rf2.tile) < rf_agg_no_data_cells(rf2.masked)) \
            .collect()[0][0]
        self.assertTrue(result)

        rf3 = rf1.select(rf1.tile, rf_inverse_mask_by_value(rf1.tile, rf1.mask, lit(mask_value)).alias('masked'))
        result = rf3.agg(rf_agg_no_data_cells(rf3.tile) < rf_agg_no_data_cells(rf3.masked)) \
            .collect()[0][0]
        self.assertTrue(result)

    def test_resample(self):
        from pyspark.sql.functions import lit
        result = self.rf.select(
            rf_tile_min(rf_local_equal(
                rf_resample(rf_resample(self.rf.tile, lit(2)), lit(0.5)),
                self.rf.tile))
        ).collect()[0][0]

        self.assertTrue(result == 1)  # short hand for all values are true

    def test_exists_for_all(self):
        df = self.rf.withColumn('should_exist', rf_make_ones_tile(5, 5, 'int8')) \
            .withColumn('should_not_exist', rf_make_zeros_tile(5, 5, 'int8'))

        should_exist = df.select(rf_exists(df.should_exist).alias('se')).take(1)[0].se
        self.assertTrue(should_exist)

        should_not_exist = df.select(rf_exists(df.should_not_exist).alias('se')).take(1)[0].se
        self.assertTrue(not should_not_exist)

        self.assertTrue(df.select(rf_for_all(df.should_exist).alias('se')).take(1)[0].se)
        self.assertTrue(not df.select(rf_for_all(df.should_not_exist).alias('se')).take(1)[0].se)

    def test_cell_type_in_functions(self):
        from pyrasterframes.rf_types import CellType
        ct = CellType.float32()
        ct_str = CellType.float32().with_no_data_value(-999).cell_type_name  # NB int ND into float celltype

        df = self.rf.withColumn('ct_str', rf_convert_cell_type('tile', ct_str)) \
                    .withColumn('ct', rf_convert_cell_type('tile', ct)) \
                    .withColumn('make', rf_make_constant_tile(99, 3, 4, CellType.int8()))

        result = df.select('ct', 'ct_str', 'make').first()

        self.assertEqual(result['ct'].cell_type, ct)
        self.assertEqual(result['ct_str'].cell_type, CellType(ct_str))
        self.assertEqual(result['make'].cell_type, CellType.int8())

    def test_render_png(self):
        cat = self.spark.createDataFrame([
            Row(red=self.l8band_uri(4), green=self.l8band_uri(3), blue=self.l8band_uri(2))
        ])

        rf = self.spark.read.raster(catalog = cat, catalog_col_names=['red', 'green', 'blue'])
        png_bytes = rf.select(rf_render_png('red', 'green', 'blue').alias('png')).first()['png']
        # Look for the PNG magic cookie
        self.assertEqual(png_bytes[0:8], bytearray([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]))
