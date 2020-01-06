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

from unittest import skip

import numpy as np
import sys
from numpy.testing import assert_equal
from pyspark import Row
from pyspark.sql.functions import *

import pyrasterframes
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *
from pyrasterframes.utils import gdal_version
from . import TestEnvironment


class RasterFunctions(TestEnvironment):

    def setUp(self):
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")
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
        rf2 = rf1.select(rf1.tile, rf_mask_by_value(rf1.tile, rf1.mask, lit(mask_value), False).alias('masked'))
        result = rf2.agg(rf_agg_no_data_cells(rf2.tile) < rf_agg_no_data_cells(rf2.masked)) \
            .collect()[0][0]
        self.assertTrue(result)

        # note supplying a `int` here, not a column to mask value
        rf3 = rf1.select(
            rf1.tile,
            rf_inverse_mask_by_value(rf1.tile, rf1.mask, mask_value).alias('masked'),
            rf_mask_by_value(rf1.tile, rf1.mask, mask_value, True).alias('masked2'),
        )
        result = rf3.agg(
            rf_agg_no_data_cells(rf3.tile) < rf_agg_no_data_cells(rf3.masked),
            rf_agg_no_data_cells(rf3.tile) < rf_agg_no_data_cells(rf3.masked2),
        ) \
            .first()
        self.assertTrue(result[0])
        self.assertTrue(result[1])  # inverse mask arg gives equivalent result

        result_equiv_tiles = rf3.select(rf_for_all(rf_local_equal(rf3.masked, rf3.masked2))).first()[0]
        self.assertTrue(result_equiv_tiles)  # inverse fn and inverse arg produce same Tile

    def test_mask_by_values(self):

        tile = Tile(np.random.randint(1, 100, (5, 5)), CellType.uint8())
        mask_tile = Tile(np.array(range(1, 26), 'uint8').reshape(5, 5))
        expected_diag_nd = Tile(np.ma.masked_array(tile.cells, mask=np.eye(5)))

        df = self.spark.createDataFrame([Row(t=tile, m=mask_tile)]) \
            .select(rf_mask_by_values('t', 'm', [0, 6, 12, 18, 24]))  # values on the diagonal
        result0 = df.first()
        # assert_equal(result0[0].cells, expected_diag_nd)
        self.assertTrue(result0[0] == expected_diag_nd)

    def test_mask_bits(self):
        t = Tile(42 * np.ones((4, 4), 'uint16'), CellType.uint16())
        # with a varitey of known values
        mask = Tile(np.array([
            [1, 1, 2720, 2720],
            [1, 6816, 6816, 2756],
            [2720, 2720, 6900, 2720],
            [2720, 6900, 6816, 1]
        ]), CellType('uint16raw'))

        df = self.spark.createDataFrame([Row(t=t, mask=mask)])

        # removes fill value 1
        mask_fill_df = df.select(rf_mask_by_bit('t', 'mask', 0, True).alias('mbb'))
        mask_fill_tile = mask_fill_df.first()['mbb']

        self.assertTrue(mask_fill_tile.cell_type.has_no_data())

        self.assertTrue(
            mask_fill_df.select(rf_data_cells('mbb')).first()[0],
            16 - 4
        )

        # mask out 6816, 6900
        mask_med_hi_cir = df.withColumn('mask_cir_mh',
                                        rf_mask_by_bits('t', 'mask', 11, 2, [2, 3])) \
            .first()['mask_cir_mh'].cells

        self.assertEqual(
            mask_med_hi_cir.mask.sum(),
            5
        )

    @skip('Issue #422 https://github.com/locationtech/rasterframes/issues/422')
    def test_mask_and_deser(self):
        # duplicates much of test_mask_bits but
        t = Tile(42 * np.ones((4, 4), 'uint16'), CellType.uint16())
        # with a varitey of known values
        mask = Tile(np.array([
            [1, 1, 2720, 2720],
            [1, 6816, 6816, 2756],
            [2720, 2720, 6900, 2720],
            [2720, 6900, 6816, 1]
        ]), CellType('uint16raw'))

        df = self.spark.createDataFrame([Row(t=t, mask=mask)])

        # removes fill value 1
        mask_fill_df = df.select(rf_mask_by_bit('t', 'mask', 0, True).alias('mbb'))
        mask_fill_tile = mask_fill_df.first()['mbb']

        self.assertTrue(mask_fill_tile.cell_type.has_no_data())

        # Unsure why this fails. mask_fill_tile.cells is all 42 unmasked.
        self.assertEqual(mask_fill_tile.cells.mask.sum(), 4,
                         f'Expected {16 - 4} data values but got the masked tile:'
                         f'{mask_fill_tile}'
                         )

    def test_mask(self):
        from pyspark.sql import Row
        from pyrasterframes.rf_types import Tile, CellType

        np.random.seed(999)
        # importantly exclude 0 from teh range because that's the nodata value for the `data_tile`'s cell type
        ma = np.ma.array(np.random.randint(1, 10, (5, 5), dtype='int8'), mask=np.random.rand(5, 5) > 0.7)
        expected_data_values = ma.compressed().size
        expected_no_data_values = ma.size - expected_data_values
        self.assertTrue(expected_data_values > 0, "Make sure random seed is cooperative ")
        self.assertTrue(expected_no_data_values > 0, "Make sure random seed is cooperative ")

        data_tile = Tile(np.ones(ma.shape, ma.dtype), CellType.uint8())

        df = self.spark.createDataFrame([Row(t=data_tile, m=Tile(ma))]) \
            .withColumn('masked_t', rf_mask('t', 'm'))

        result = df.select(rf_data_cells('masked_t')).first()[0]
        self.assertEqual(result, expected_data_values,
                         f"Masked tile should have {expected_data_values} data values but found: {df.select('masked_t').first()[0].cells}."
                         f"Original data: {data_tile.cells}"
                         f"Masked by {ma}")

        nd_result = df.select(rf_no_data_cells('masked_t')).first()[0]
        self.assertEqual(nd_result, expected_no_data_values)

        # deser of tile is correct
        self.assertEqual(
            df.select('masked_t').first()[0].cells.compressed().size,
            expected_data_values
        )

    def test_extract_bits(self):
        one = np.ones((6, 6), 'uint8')
        t = Tile(84 * one)
        df = self.spark.createDataFrame([Row(t=t)])
        result_py_literals = df.select(rf_local_extract_bits('t', 2, 3)).first()[0]
        # expect value binary 84 => 1010100 => 101
        assert_equal(result_py_literals.cells, 5 * one)

        result_cols = df.select(rf_local_extract_bits('t', lit(2), lit(3))).first()[0]
        assert_equal(result_cols.cells, 5 * one)

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
        ct = CellType.float32().with_no_data_value(-999)

        df = self.rf.withColumn('ct_str', rf_convert_cell_type('tile', ct.cell_type_name)) \
            .withColumn('ct', rf_convert_cell_type('tile', ct)) \
            .withColumn('make', rf_make_constant_tile(99, 3, 4, CellType.int8())) \
            .withColumn('make2', rf_with_no_data('make', 99))

        result = df.select('ct', 'ct_str', 'make', 'make2').first()

        self.assertEqual(result['ct'].cell_type, ct)
        self.assertEqual(result['ct_str'].cell_type, ct)
        self.assertEqual(result['make'].cell_type, CellType.int8())

        counts = df.select(
            rf_no_data_cells('make').alias("nodata1"),
            rf_data_cells('make').alias("data1"),
            rf_no_data_cells('make2').alias("nodata2"),
            rf_data_cells('make2').alias("data2")
        ).first()

        self.assertEqual(counts["data1"], 3 * 4)
        self.assertEqual(counts["nodata1"], 0)
        self.assertEqual(counts["data2"], 0)
        self.assertEqual(counts["nodata2"], 3 * 4)
        self.assertEqual(result['make2'].cell_type, CellType.int8().with_no_data_value(99))

    def test_render_composite(self):
        cat = self.spark.createDataFrame([
            Row(red=self.l8band_uri(4), green=self.l8band_uri(3), blue=self.l8band_uri(2))
        ])
        rf = self.spark.read.raster(cat, catalog_col_names=cat.columns)

        # Test composite construction
        rgb = rf.select(rf_tile(rf_rgb_composite('red', 'green', 'blue')).alias('rgb')).first()['rgb']

        # TODO: how to better test this?
        self.assertIsInstance(rgb, Tile)
        self.assertEqual(rgb.dimensions(), [186, 169])

        ## Test PNG generation
        png_bytes = rf.select(rf_render_png('red', 'green', 'blue').alias('png')).first()['png']
        # Look for the PNG magic cookie
        self.assert_png(png_bytes)

    def test_rf_interpret_cell_type_as(self):
        from pyspark.sql import Row
        from pyrasterframes.rf_types import Tile

        df = self.spark.createDataFrame([
            Row(t=Tile(np.array([[1, 3, 4], [5, 0, 3]]), CellType.uint8().with_no_data_value(5)))
        ])
        df = df.withColumn('tile', rf_interpret_cell_type_as('t', 'uint8ud3'))  # threes become ND
        result = df.select(rf_tile_sum(rf_local_equal('t', lit(3))).alias('threes')).first()['threes']
        self.assertEqual(result, 2)

        result_5 = df.select(rf_tile_sum(rf_local_equal('t', lit(5))).alias('fives')).first()['fives']
        self.assertEqual(result_5, 0)

    def test_rf_local_data_and_no_data(self):
        from pyspark.sql import Row
        from pyrasterframes.rf_types import Tile

        nd = 5
        t = Tile(
            np.array([[1, 3, 4], [nd, 0, 3]]),
            CellType.uint8().with_no_data_value(nd))
        # note the convert is due to issue #188
        df = self.spark.createDataFrame([Row(t=t)])\
            .withColumn('lnd', rf_convert_cell_type(rf_local_no_data('t'), 'uint8')) \
            .withColumn('ld',  rf_convert_cell_type(rf_local_data('t'),    'uint8'))

        result = df.first()
        result_nd = result['lnd']
        assert_equal(result_nd.cells, t.cells.mask)

        result_d = result['ld']
        assert_equal(result_d.cells, np.invert(t.cells.mask))

    def test_rf_local_is_in(self):
        from pyspark.sql.functions import lit, array, col
        from pyspark.sql import Row

        nd = 5
        t = Tile(
            np.array([[1, 3, 4], [nd, 0, 3]]),
            CellType.uint8().with_no_data_value(nd))
        # note the convert is due to issue #188
        df = self.spark.createDataFrame([Row(t=t)]) \
            .withColumn('a', array(lit(3), lit(4))) \
            .withColumn('in2', rf_convert_cell_type(
                rf_local_is_in(col('t'), array(lit(0), lit(4))),
                'uint8')) \
            .withColumn('in3', rf_convert_cell_type(rf_local_is_in('t', 'a'), 'uint8')) \
            .withColumn('in4', rf_convert_cell_type(
                rf_local_is_in('t', array(lit(0), lit(4), lit(3))),
                'uint8')) \
            .withColumn('in_list', rf_convert_cell_type(rf_local_is_in(col('t'), [4, 1]), 'uint8'))

        result = df.first()
        self.assertEqual(result['in2'].cells.sum(), 2)
        assert_equal(result['in2'].cells, np.isin(t.cells, np.array([0, 4])))
        self.assertEqual(result['in3'].cells.sum(), 3)
        self.assertEqual(result['in4'].cells.sum(), 4)
        self.assertEqual(result['in_list'].cells.sum(), 2,
                         "Tile value {} should contain two 1s as: [[1, 0, 1],[0, 0, 0]]"
                         .format(result['in_list'].cells))

    def test_rf_agg_overview_raster(self):
        width = 500
        height = 400
        agg = self.prdf.select(rf_agg_extent(rf_extent(self.prdf.proj_raster)).alias("extent")).first().extent
        crs = self.prdf.select(rf_crs(self.prdf.proj_raster).alias("crs")).first().crs.crsProj4
        aoi = Extent.from_row(agg)
        aoi = aoi.reproject(crs, "EPSG:3857")
        aoi = aoi.buffer(-(aoi.width * 0.2))

        ovr = self.prdf.select(rf_agg_overview_raster(self.prdf.proj_raster, width, height, aoi).alias("agg"))
        png = ovr.select(rf_render_color_ramp_png('agg', 'Greyscale64')).first()[0]
        self.assert_png(png)

        # with open('/tmp/test_rf_agg_overview_raster.png', 'wb') as f:
        #     f.write(png)

    def test_rf_proj_raster(self):
        df = self.prdf.select(rf_proj_raster(rf_tile('proj_raster'),
                                             rf_extent('proj_raster'),
                                             rf_crs('proj_raster')).alias('roll_your_own'))
        'tile_context' in df.schema['roll_your_own'].dataType.fieldNames()

