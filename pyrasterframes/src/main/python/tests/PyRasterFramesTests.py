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

import unittest

import numpy as np
import geomesa_pyspark.types
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *
from pyspark.sql import SQLContext, Column
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

    def test_rasterize(self):
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

        cols = 194
        rows = 250
        with_raster = with_poly.withColumn('rasterized', rf_rasterize('poly', 'geometry', lit(16), cols, rows))
        # expect a 4 by 4 cell
        result = with_raster.select(rf_tile_sum(rf_local_equal_int(with_raster.rasterized, 16)),
                                    rf_tile_sum(with_raster.rasterized))
        expected_burned_in_cells = (cols - 2 * buf_cells) * (rows - 2 * buf_cells)
        self.assertEqual(result.first()[0], float(expected_burned_in_cells))
        self.assertEqual(result.first()[1], 16. * expected_burned_in_cells)

    def test_reproject(self):
        reprojected = self.rf.withColumn('reprojected', st_reproject('center', 'EPSG:4326', 'EPSG:3857'))
        reprojected.show()
        self.assertEqual(reprojected.count(), 8)

    def test_geojson(self):
        import os
        sample = 'file://' + os.path.join(self.resource_dir, 'buildings.geojson')
        geo = self.spark.read.geojson(sample)
        geo.show()
        self.assertEqual(geo.select('geometry').count(), 8)


class RasterFunctions(TestEnvironment):

    def setUp(self):
        self.create_layer()

    def test_setup(self):
        self.assertEqual(self.spark.sparkContext.getConf().get("spark.serializer"),
                         "org.apache.spark.serializer.KryoSerializer")

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
        base = self.spark.createDataFrame([1, 2, 3, 4], 'integer')
        tiles = base.select(rf_make_constant_tile(3, 3, 3, "int32"), rf_make_zeros_tile(3, 3, "int32"),
                            rf_make_ones_tile(3, 3, "int32"))
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
                            rf_local_divide(rf_convert_cell_type(tile, "float32"), 2) AS over_two 
                        FROM rf_test_sql""")\

        arith.createOrReplaceTempView('rf_test_sql_1')
        arith.show(truncate=False)
        stats = self.spark.sql("""
        SELECT rf_tile_mean(tile) as base,
            rf_tile_mean(add_one) as plus_one,
            rf_tile_mean(less_one) as minus_one,
            rf_tile_mean(times_two) as double,
            rf_tile_mean(over_two) as half
        FROM rf_test_sql_1
        """)
        stats.show(truncate=False)
        stats.createOrReplaceTempView('rf_test_sql_stats')

        compare = self.spark.sql("""
        SELECT 
            plus_one - 1.0 = base as add,
            minus_one + 1.0 = base as subtract,
            double / 2.0 = base as multiply,
            half * 2.0 = base as divide
        FROM rf_test_sql_stats
        """)

        expect_row = compare.first()

        self.assertTrue(expect_row.add)
        self.assertTrue(expect_row.subtract)
        self.assertTrue(expect_row.multiply)
        self.assertTrue(expect_row.divide)

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


class CellTypeHandling(unittest.TestCase):

    def test_is_raw(self):
        self.assertTrue(CellType("float32raw").is_raw())
        self.assertFalse(CellType("float64ud1234").is_raw())
        self.assertFalse(CellType("float32").is_raw())
        self.assertTrue(CellType("int8raw").is_raw())
        self.assertFalse(CellType("uint16d12").is_raw())
        self.assertFalse(CellType("int32").is_raw())

    def test_is_floating_point(self):
        self.assertTrue(CellType("float32raw").is_floating_point())
        self.assertTrue(CellType("float64ud1234").is_floating_point())
        self.assertTrue(CellType("float32").is_floating_point())
        self.assertFalse(CellType("int8raw").is_floating_point())
        self.assertFalse(CellType("uint16d12").is_floating_point())
        self.assertFalse(CellType("int32").is_floating_point())

    def test_cell_type_no_data(self):
        import math
        self.assertIsNone(CellType.bool().no_data_value())

        self.assertTrue(CellType.int8().has_no_data())
        self.assertEqual(CellType.int8().no_data_value(), -128)

        self.assertTrue(CellType.uint8().has_no_data())
        self.assertEqual(CellType.uint8().no_data_value(), 0)

        self.assertTrue(CellType.int16().has_no_data())
        self.assertEqual(CellType.int16().no_data_value(), -32768)

        self.assertTrue(CellType.uint16().has_no_data())
        self.assertEqual(CellType.uint16().no_data_value(), 0)

        self.assertTrue(CellType.float32().has_no_data())
        self.assertTrue(np.isnan(CellType.float32().no_data_value()))

        self.assertEqual(CellType("float32ud-98").no_data_value(), -98.0)
        self.assertTrue(math.isnan(CellType.float64().no_data_value()))
        self.assertEqual(CellType.uint8().no_data_value(), 0)

    def test_cell_type_conversion(self):
        for ct in rf_cell_types():
            self.assertEqual(ct.to_numpy_dtype(),
                             CellType.from_numpy_dtype(ct.to_numpy_dtype()).to_numpy_dtype(),
                             "dtype comparison for " + str(ct))
            if not ct.is_raw():
                self.assertEqual(ct,
                                 CellType.from_numpy_dtype(ct.to_numpy_dtype()),
                                 "GTCellType comparison for " + str(ct))
            else:
                ct_ud = ct.with_no_data_value(99)
                self.assertEqual(ct_ud.base_cell_type_name(),
                                 repr(CellType.from_numpy_dtype(ct_ud.to_numpy_dtype())),
                                 "GTCellType comparison for " + str(ct_ud)
                                 )


class UDT(TestEnvironment):

    def setUp(self):
        self.create_layer()

    def test_mask_no_data(self):
        t1 = Tile(np.array([[1, 2], [3, 4]]), CellType("int8ud3"))
        self.assertTrue(t1.cells.mask[1][0])
        self.assertIsNotNone(t1.cells[1][1])
        self.assertEqual(len(t1.cells.compressed()), 3)

        t2 = Tile(np.array([[1.0, 2.0], [float('nan'), 4.0]]), CellType.float32())
        self.assertEqual(len(t2.cells.compressed()), 3)
        self.assertTrue(t2.cells.mask[1][0])
        self.assertIsNotNone(t2.cells[1][1])

    def test_tile_udt_serialization(self):
        from pyspark.sql.types import StructType, StructField

        udt = TileUDT()
        cell_types = (ct for ct in rf_cell_types() if not (ct.is_raw() or ("bool" in ct.base_cell_type_name())))

        for ct in cell_types:
            cells = (100 + np.random.randn(3, 3) * 100).astype(ct.to_numpy_dtype())

            if ct.is_floating_point():
                nd = 33.0
            else:
                nd = 33

            cells[1][1] = nd
            a_tile = Tile(cells, ct.with_no_data_value(nd))
            round_trip = udt.fromInternal(udt.toInternal(a_tile))
            self.assertEquals(a_tile, round_trip, "round-trip serialization for " + str(ct))

            schema = StructType([StructField("tile", TileUDT(), False)])
            df = self.spark.createDataFrame([{"tile": a_tile}], schema)

            long_trip = df.first()["tile"]
            self.assertEqual(long_trip, a_tile)

    def test_udf_on_tile_type_input(self):
        import numpy.testing
        df = self.spark.read.raster(self.img_uri)
        rf = self.rf

        # create trivial UDF that does something we already do with raster_Functions
        @udf('integer')
        def my_udf(t):
            a = t.cells
            return a.size  # same as rf_dimensions.cols * rf_dimensions.rows

        rf_result = rf.select(
            (rf_dimensions('tile').cols.cast('int') * rf_dimensions('tile').rows.cast('int')).alias('expected'),
            my_udf('tile').alias('result')).toPandas()

        numpy.testing.assert_array_equal(
            rf_result.expected.tolist(),
            rf_result.result.tolist()
        )

        df_result = df.select(
            (rf_dimensions(df.proj_raster).cols.cast('int') * rf_dimensions(df.proj_raster).rows.cast('int') -
                my_udf(rf_tile(df.proj_raster))).alias('result')
        ).toPandas()

        numpy.testing.assert_array_equal(
            np.zeros(len(df_result)),
            df_result.result.tolist()
        )

    def test_udf_on_tile_type_output(self):
        import numpy.testing

        rf = self.rf

        # create a trivial UDF that does something we already do with a raster_functions
        @udf(TileUDT())
        def my_udf(t):
            import numpy as np
            return Tile(np.log1p(t.cells))

        rf_result = rf.select(
            rf_tile_max(
                rf_local_subtract(
                    my_udf(rf.tile),
                    rf_log1p(rf.tile)
                )
            ).alias('expect_zeros')
        ).collect()

        # almost equal because of different implemenations under the hoods: C (numpy) versus Java (rf_)
        numpy.testing.assert_almost_equal(
            [r['expect_zeros'] for r in rf_result],
            [0.0 for _ in rf_result],
            decimal=6
        )

    def test_no_data_udf_handling(self):
        from pyspark.sql.types import StructType, StructField

        t1 = Tile(np.array([[1, 2], [0, 4]]), CellType.uint8())
        self.assertEqual(t1.cell_type.to_numpy_dtype(), np.dtype("uint8"))
        e1 = Tile(np.array([[2, 3], [0, 5]]), CellType.uint8())
        schema = StructType([StructField("tile", TileUDT(), False)])
        df = self.spark.createDataFrame([{"tile": t1}], schema)

        @udf(TileUDT())
        def increment(t):
            return t + 1

        r1 = df.select(increment(df.tile).alias("inc")).first()["inc"]
        self.assertEqual(r1, e1)

    def test_udf_np_implicit_type_conversion(self):
        import math
        import pandas

        a1 = np.array([[1, 2], [0, 4]])
        t1 = Tile(a1, CellType.uint8())
        exp_array = a1.astype('>f8')

        @udf(TileUDT())
        def times_pi(t):
            return t * math.pi

        @udf(TileUDT())
        def divide_pi(t):
            return t / math.pi

        @udf(TileUDT())
        def plus_pi(t):
            return t + math.pi

        @udf(TileUDT())
        def less_pi(t):
            return t - math.pi

        df = self.spark.createDataFrame(pandas.DataFrame([{"tile": t1}]))
        r1 = df.select(
            less_pi(divide_pi(times_pi(plus_pi(df.tile))))
        ).first()[0]

        self.assertTrue(np.all(r1.cells == exp_array))
        self.assertEqual(r1.cells.dtype, exp_array.dtype)


class TileOps(TestEnvironment):

    def setUp(self):
        # convenience so we can assert around Tile() == Tile()
        self.t1 = Tile(np.array([[1, 2],
                                 [3, 4]]), CellType.int8().with_no_data_value(3))
        self.t2 = Tile(np.array([[1, 2],
                                 [3, 4]]), CellType.int8().with_no_data_value(1))

    def test_addition(self):
        e1 = np.ma.masked_equal(np.array([[5, 6],
                                          [7, 8]]), 7)
        self.assertTrue(np.array_equal((self.t1 + 4).cells, e1))

        e2 = np.ma.masked_equal(np.array([[3, 4],
                                          [3, 8]]), 3)
        r2 = (self.t1 + self.t2).cells
        self.assertTrue(np.ma.allequal(r2, e2))

    def test_multiplication(self):
        e1 = np.ma.masked_equal(np.array([[4, 8],
                                          [12, 16]]), 12)

        self.assertTrue(np.array_equal((self.t1 * 4).cells, e1))

        e2 = np.ma.masked_equal(np.array([[3, 4], [3, 16]]), 3)
        r2 = (self.t1 * self.t2).cells
        self.assertTrue(np.ma.allequal(r2, e2))

    def test_subtraction(self):
        t3 = self.t1 * 4
        r1 = t3 - self.t1
        # note careful construction of mask value and dtype above
        e1 = Tile(np.ma.masked_equal(np.array([[4 - 1, 8 - 2],
                                               [3, 16 - 4]], dtype='int8'),
                                     3, )
                  )
        self.assertTrue(r1 == e1,
                        "{} does not equal {}".format(r1, e1))
        # put another way
        self.assertTrue(r1 == self.t1 * 3,
                        "{} does not equal {}".format(r1, self.t1 * 3))

    def test_division(self):
        t3 = self.t1 * 9
        r1 = t3 / 9
        self.assertTrue(np.array_equal(r1.cells, self.t1.cells),
                        "{} does not equal {}".format(r1, self.t1))

        r2 = (self.t1 / self.t1).cells
        self.assertTrue(np.array_equal(r2, np.array([[1,1], [1, 1]], dtype=r2.dtype)))

    def test_matmul(self):
        # if sys.version >= '3.5':  # per https://docs.python.org/3.7/library/operator.html#operator.matmul new in 3.5
        #     r1 = self.t1 @ self.t2
        r1 = self.t1.__matmul__(self.t2)

        # The behavior of np.matmul with masked arrays is not well documented
        # it seems to treat the 2nd arg as if not a MaskedArray
        e1 = Tile(np.matmul(self.t1.cells, self.t2.cells), r1.cell_type)

        self.assertTrue(r1 == e1, "{} was not equal to {}".format(r1, e1))
        self.assertEqual(r1, e1)


class PandasInterop(TestEnvironment):

    def setUp(self):
        self.create_layer()

    def test_pandas_conversion(self):
        import pandas as pd
        # pd.options.display.max_colwidth = 256
        cell_types = (ct for ct in rf_cell_types() if not (ct.is_raw() or ("bool" in ct.base_cell_type_name())))
        tiles = [Tile(np.random.randn(5, 5) * 100, ct) for ct in cell_types]
        in_pandas = pd.DataFrame({
            'tile': tiles
        })

        in_spark = self.spark.createDataFrame(in_pandas)
        out_pandas = in_spark.select(rf_identity('tile').alias('tile')).toPandas()
        self.assertTrue(out_pandas.equals(in_pandas), str(in_pandas) + "\n\n" + str(out_pandas))

    def test_extended_pandas_ops(self):
        import pandas as pd

        self.assertIsInstance(self.rf.sql_ctx, SQLContext)

        # Try to collect self.rf which is read from a geotiff
        rf_collect = self.rf.take(2)
        self.assertTrue(
            all([isinstance(row.tile.cells, np.ndarray) for row in rf_collect]))

        # Try to create a tile from numpy.
        self.assertEqual(Tile(np.random.randn(10, 10), CellType.int8()).dimensions(), [10, 10])

        tiles = [Tile(np.random.randn(10, 12), CellType.float64()) for _ in range(3)]
        to_spark = pd.DataFrame({
            't': tiles,
            'b': ['a', 'b', 'c'],
            'c': [1, 2, 4],
        })
        rf_maybe = self.spark.createDataFrame(to_spark)

        # rf_maybe.select(rf_render_matrix(rf_maybe.t)).show(truncate=False)

        # Try to do something with it.
        sums = to_spark.t.apply(lambda a: a.cells.sum()).tolist()
        maybe_sums = rf_maybe.select(rf_tile_sum(rf_maybe.t).alias('tsum'))
        maybe_sums = [r.tsum for r in maybe_sums.collect()]
        np.testing.assert_almost_equal(maybe_sums, sums, 12)

        # Test round trip for an array
        simple_array = Tile(np.array([[1, 2], [3, 4]]), CellType.float64())
        to_spark_2 = pd.DataFrame({
            't': [simple_array]
        })

        rf_maybe_2 = self.spark.createDataFrame(to_spark_2)
        #print("RasterFrameLayer `show`:")
        #rf_maybe_2.select(rf_render_matrix(rf_maybe_2.t).alias('t')).show(truncate=False)

        pd_2 = rf_maybe_2.toPandas()
        array_back_2 = pd_2.iloc[0].t
        #print("Array collected from toPandas output\n", array_back_2)

        self.assertIsInstance(array_back_2, Tile)
        np.testing.assert_equal(array_back_2.cells, simple_array.cells)


class RasterJoin(TestEnvironment):

    def setUp(self):
        self.create_layer()

    def test_raster_join(self):
        # re-read the same source
        rf_prime = self.spark.read.geotiff(self.img_uri) \
            .withColumnRenamed('tile', 'tile2').alias('rf_prime')

        rf_joined = self.rf.raster_join(rf_prime)

        self.assertTrue(rf_joined.count(), self.rf.count())
        self.assertTrue(len(rf_joined.columns) == len(self.rf.columns) + len(rf_prime.columns) - 2)

        rf_joined_2 = self.rf.raster_join(rf_prime, self.rf.extent, self.rf.crs, rf_prime.extent, rf_prime.crs)
        self.assertTrue(rf_joined_2.count(), self.rf.count())
        self.assertTrue(len(rf_joined_2.columns) == len(self.rf.columns) + len(rf_prime.columns) - 2)

        # this will bring arbitrary additional data into join; garbage result
        join_expression = self.rf.extent.xmin == rf_prime.extent.xmin
        rf_joined_3 = self.rf.raster_join(rf_prime, self.rf.extent, self.rf.crs,
                                          rf_prime.extent, rf_prime.crs,
                                          join_expression)
        self.assertTrue(rf_joined_3.count(), self.rf.count())
        self.assertTrue(len(rf_joined_3.columns) == len(self.rf.columns) + len(rf_prime.columns) - 2)

        # throws if you don't  pass  in all expected columns
        with self.assertRaises(AssertionError):
            self.rf.raster_join(rf_prime, join_exprs=self.rf.extent)


class RasterSource(TestEnvironment):

    def test_handle_lazy_eval(self):
        df = self.spark.read.raster(self.img_uri)
        ltdf = df.select('proj_raster')
        self.assertGreater(ltdf.count(), 0)
        self.assertIsNotNone(ltdf.first())

        tdf = df.select(rf_tile('proj_raster'))
        self.assertGreater(tdf.count(),  0)
        self.assertIsNotNone(tdf.first())

    def test_strict_eval(self):
        df_lazy = self.spark.read.raster(self.img_uri, lazy_tiles=True)
        # when doing Show on a lazy tile we will see something like RasterRefTile(RasterRef(JVMGeoTiffRasterSource(...
        # use this trick to get the `show` string
        show_str_lazy = df_lazy.select('proj_raster')._jdf.showString(1, -1, False)
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

    def test_raster_source_reader(self):
        # much the same as RasterSourceDataSourceSpec here; but using https PDS. Takes about 30s to run

        def l8path(b):
            assert b in range(1, 12)
            base = "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/199/026/LC08_L1TP_199026_20180919_20180928_01_T1/LC08_L1TP_199026_20180919_20180928_01_T1_B{}.TIF"
            return base.format(b)

        path_param = '\n'.join([l8path(b) for b in [1, 2, 3]])  # "http://foo.com/file1.tif,http://foo.com/file2.tif"
        tile_size = 512

        df = self.spark.read.raster(
            tile_dimensions=(tile_size, tile_size),
            paths=path_param,
            lazy_tiles=True,
        ).cache()

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
        print(path_count.toPandas())
        self.assertTrue(path_count.count() == 3)

    def test_raster_source_catalog_reader(self):
        import pandas as pd

        scene_dict = {
            1: 'http://landsat-pds.s3.amazonaws.com/c1/L8/015/041/LC08_L1TP_015041_20190305_20190309_01_T1/LC08_L1TP_015041_20190305_20190309_01_T1_B{}.TIF',
            2: 'http://landsat-pds.s3.amazonaws.com/c1/L8/015/042/LC08_L1TP_015042_20190305_20190309_01_T1/LC08_L1TP_015042_20190305_20190309_01_T1_B{}.TIF',
            3: 'http://landsat-pds.s3.amazonaws.com/c1/L8/016/041/LC08_L1TP_016041_20190224_20190309_01_T1/LC08_L1TP_016041_20190224_20190309_01_T1_B{}.TIF',
        }

        def path(scene, band):
            assert band in range(1, 12)
            p = scene_dict[scene]
            return p.format(band)

        # Create a pandas dataframe (makes it easy to create spark df)
        path_pandas = pd.DataFrame([
            {'b1': path(1, 1), 'b2': path(1, 2), 'b3': path(1, 3)},
            {'b1': path(2, 1), 'b2': path(2, 2), 'b3': path(2, 3)},
            {'b1': path(3, 1), 'b2': path(3, 2), 'b3': path(3, 3)},
        ])
        # comma separated list of column names containing URI's to read.
        catalog_columns = ','.join(path_pandas.columns.tolist())  # 'b1,b2,b3'
        path_table = self.spark.createDataFrame(path_pandas)

        path_df = self.spark.read.raster(
            tile_dimensions=(512, 512),
            catalog=path_table,
            catalog_col_names=catalog_columns,
            lazy_tiles=True # We'll get an OOM error if we try to read 9 scenes all at once!
        )

        self.assertTrue(len(path_df.columns) == 6)  # three bands times {path, tile}
        self.assertTrue(path_df.select('b1_path').distinct().count() == 3)  # as per scene_dict
        b1_paths_maybe = path_df.select('b1_path').distinct().collect()
        b1_paths = [s.format('1') for s in scene_dict.values()]
        self.assertTrue(all([row.b1_path in b1_paths for row in b1_paths_maybe]))

    def test_raster_source_catalog_reader_with_pandas(self):
        import pandas as pd
        import geopandas
        from shapely.geometry import Point

        scene_dict = {
            1: 'http://landsat-pds.s3.amazonaws.com/c1/L8/015/041/LC08_L1TP_015041_20190305_20190309_01_T1/LC08_L1TP_015041_20190305_20190309_01_T1_B{}.TIF',
            2: 'http://landsat-pds.s3.amazonaws.com/c1/L8/015/042/LC08_L1TP_015042_20190305_20190309_01_T1/LC08_L1TP_015042_20190305_20190309_01_T1_B{}.TIF',
            3: 'http://landsat-pds.s3.amazonaws.com/c1/L8/016/041/LC08_L1TP_016041_20190224_20190309_01_T1/LC08_L1TP_016041_20190224_20190309_01_T1_B{}.TIF',
        }

        def path(scene, band):
            assert band in range(1, 12)
            p = scene_dict[scene]
            return p.format(band)

        # Create a pandas dataframe (makes it easy to create spark df)
        path_pandas = pd.DataFrame([
            {'b1': path(1, 1), 'b2': path(1, 2), 'b3': path(1, 3), 'geo': Point(1, 1)},
            {'b1': path(2, 1), 'b2': path(2, 2), 'b3': path(2, 3), 'geo': Point(2, 2)},
            {'b1': path(3, 1), 'b2': path(3, 2), 'b3': path(3, 3), 'geo': Point(3, 3)},
        ])

        # here a subtle difference with the test_raster_source_catalog_reader test, feed the DataFrame not a CSV and not an already created spark DF.
        df = self.spark.read.raster(
            catalog=path_pandas,
            catalog_col_names=['b1', 'b2', 'b3']
        )
        self.assertEqual(len(df.columns), 7)  # three path cols, three tile cols, and geo
        self.assertTrue('geo' in df.columns)
        self.assertTrue(df.select('b1_path').distinct().count() == 3)


        # Same test with geopandas
        geo_df = geopandas.GeoDataFrame(path_pandas, crs={'init': 'EPSG:4326'}, geometry='geo')
        df2 = self.spark.read.raster(
            catalog=geo_df,
            catalog_col_names=['b1', 'b2', 'b3']
        )
        self.assertEqual(len(df2.columns), 7)  # three path cols, three tile cols, and geo
        self.assertTrue('geo' in df2.columns)
        self.assertTrue(df2.select('b1_path').distinct().count() == 3)


def suite():
    function_tests = unittest.TestSuite()
    return function_tests


unittest.TextTestRunner().run(suite())
