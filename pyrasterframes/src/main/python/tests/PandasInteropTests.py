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

import numpy as np
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from . import TestEnvironment



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

    def test_pandas_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.SCALAR)
        def tile_mean(cells):
            # `cells` is a Pandas `Series`.
            return cells.apply(np.mean)

        df = self.rf.select(tile_mean(rf_tensor(self.rf.tile)).alias('pandas_udf_mean'), rf_tile_mean(self.rf.tile))
        df.show(truncate=False)

