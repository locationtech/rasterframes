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
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from . import TestEnvironment


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
        self.assertEqual(CellType("float32ud-98").no_data_value(), -98)
        self.assertEqual(CellType("int32ud-98").no_data_value(), -98.0)
        self.assertEqual(CellType("int32ud-98").no_data_value(), -98)

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
        from pyspark.sql import Row
        # convenience so we can assert around Tile() == Tile()
        self.t1 = Tile(np.array([[1, 2],
                                 [3, 4]]), CellType.int8().with_no_data_value(3))
        self.t2 = Tile(np.array([[1, 2],
                                 [3, 4]]), CellType.int8().with_no_data_value(1))
        self.t3 = Tile(np.array([[1,  2],
                                 [-3, 4]]), CellType.int8().with_no_data_value(3))

        self.df = self.spark.createDataFrame([Row(t1=self.t1, t2=self.t2, t3=self.t3)])

    def test_addition(self):
        e1 = np.ma.masked_equal(np.array([[5, 6],
                                          [7, 8]]), 7)
        self.assertTrue(np.array_equal((self.t1 + 4).cells, e1))

        e2 = np.ma.masked_equal(np.array([[3, 4],
                                          [3, 8]]), 3)
        r2 = (self.t1 + self.t2).cells
        self.assertTrue(np.ma.allequal(r2, e2))

        col_result = self.df.select(rf_local_add('t1', 't3').alias('sum')).first()
        self.assertEqual(col_result.sum, self.t1 + self.t3)

    def test_multiplication(self):
        e1 = np.ma.masked_equal(np.array([[4, 8],
                                          [12, 16]]), 12)

        self.assertTrue(np.array_equal((self.t1 * 4).cells, e1))

        e2 = np.ma.masked_equal(np.array([[3, 4], [3, 16]]), 3)
        r2 = (self.t1 * self.t2).cells
        self.assertTrue(np.ma.allequal(r2, e2))

        r3 = self.df.select(rf_local_multiply('t1', 't3').alias('r3')).first().r3
        self.assertEqual(r3, self.t1 * self.t3)

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




def suite():
    function_tests = unittest.TestSuite()
    return function_tests


unittest.TextTestRunner().run(suite())
