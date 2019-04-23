
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import *
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from geomesa_pyspark.types import *
from pathlib import Path
import os
import unittest

# version-conditional imports
import sys
if sys.version_info[0] > 2:
    import builtins
else:
    import __builtin__ as builtins


def _rounded_compare(val1, val2):
    print('Comparing {} and {} using round()'.format(val1, val2))
    return builtins.round(val1) == builtins.round(val2)


class RasterFunctionsTest(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        # gather Scala requirements
        jarpath = list(Path('../target/scala-2.11').resolve().glob('pyrasterframes-assembly*.jar'))[0]

        # hard-coded relative path for resources
        cls.resource_dir = Path('./static').resolve()

        # spark session with RF
        cls.spark = (SparkSession.builder
            .config('spark.driver.extraClassPath', jarpath)
            .config('spark.executor.extraClassPath', jarpath)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "astraea.spark.rasterframes.util.RFKryoRegistrator")
            .config("spark.kryoserializer.buffer.max", "500m")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        print(cls.spark.version)
        cls.spark.withRasterFrames()

        # load something into a rasterframe
        rf = cls.spark.read.geotiff(cls.resource_dir.joinpath('L8-B8-Robinson-IL.tiff').as_uri()) \
            .withBounds() \
            .withCenter()

        # convert the tile cell type to provide for other operations
        cls.tileCol = 'tile'
        cls.rf = rf.withColumn('tile2', convert_cell_type(cls.tileCol, 'float32')) \
            .drop(cls.tileCol) \
            .withColumnRenamed('tile2', cls.tileCol).asRF()
        #cls.rf.show()


    def test_identify_columns(self):
        cols = self.rf.tileColumns()
        self.assertEqual(len(cols), 1, '`tileColumns` did not find the proper number of columns.')
        print("Tile columns: ", cols)
        col = self.rf.spatialKeyColumn()
        self.assertIsInstance(col, Column, '`spatialKeyColumn` was not found')
        print("Spatial key column: ", col)
        col = self.rf.temporalKeyColumn()
        self.assertIsNone(col, '`temporalKeyColumn` should be `None`')
        print("Temporal key column: ", col)


    def test_tile_operations(self):
        df1 = self.rf.withColumnRenamed(self.tileCol, 't1').asRF()
        df2 = self.rf.withColumnRenamed(self.tileCol, 't2').asRF()
        df3 = df1.spatialJoin(df2).asRF()
        df3 = df3.withColumn('norm_diff', normalized_difference('t1', 't2'))
        df3.printSchema()

        aggs = df3.agg(
            agg_mean('norm_diff'),
        )
        aggs.show()
        row = aggs.first()

        self.assertTrue(_rounded_compare(row['agg_mean(norm_diff)'], 0))


    def test_general(self):
        meta = self.rf.tileLayerMetadata()
        self.assertIsNotNone(meta['bounds'])
        df = self.rf.withColumn('dims',  tile_dimensions(self.tileCol)) \
            .withColumn('type', cell_type(self.tileCol)) \
            .withColumn('dCells', data_cells(self.tileCol)) \
            .withColumn('ndCells', no_data_cells(self.tileCol)) \
            .withColumn('min', tile_min(self.tileCol)) \
            .withColumn('max', tile_max(self.tileCol)) \
            .withColumn('mean', tile_mean(self.tileCol)) \
            .withColumn('sum', tile_sum(self.tileCol)) \
            .withColumn('stats', tile_stats(self.tileCol)) \
            .withColumn('envelope', envelope('geometry')) \
            .withColumn('ascii', render_ascii(self.tileCol)) \
            .withColumn('log', log(self.tileCol)) \
            .withColumn('exp', exp(self.tileCol)) \
            .withColumn('expm1', expm1(self.tileCol)) \
            .withColumn('round', round(self.tileCol))

        df.show()

    def test_rasterize(self):
        # NB: This test just makes sure rasterize runs, not that the results are correct.
        withRaster = self.rf.withColumn('rasterize', rasterize('geometry', 'geometry', lit(42), 10, 10))
        withRaster.show()

    def test_reproject(self):
        reprojected = self.rf.withColumn('reprojected', reproject_geometry('center', 'EPSG:4326', 'EPSG:3857'))
        reprojected.show()

    def test_aggregations(self):
        aggs = self.rf.agg(
            agg_mean(self.tileCol),
            agg_data_cells(self.tileCol),
            agg_no_data_cells(self.tileCol),
            agg_stats(self.tileCol),
            agg_approx_histogram(self.tileCol)
        )
        aggs.show()
        row = aggs.first()

        self.assertTrue(_rounded_compare(row['agg_mean(tile)'], 10160))
        print(row['agg_data_cells(tile)'])
        self.assertEqual(row['agg_data_cells(tile)'], 387000)
        self.assertEqual(row['agg_no_data_cells(tile)'], 1000)
        self.assertEqual(row['agg_stats(tile)'].data_cells, row['agg_data_cells(tile)'])


    def test_sql(self):

        self.rf.createOrReplaceTempView("rf")

        dims = self.rf.withColumn('dims',  tile_dimensions(self.tileCol)).first().dims
        dims_str = """{}, {}""".format(dims.cols, dims.rows)

        self.spark.sql("""SELECT tile, rf_make_constant_tile(1, {}, 'uint16') AS One, 
                            rf_make_constant_tile(2, {}, 'uint16') AS Two FROM rf""".format(dims_str, dims_str)) \
            .createOrReplaceTempView("r3")

        ops = self.spark.sql("""SELECT tile, rf_local_add(tile, One) AS AndOne, 
                                    rf_local_subtract(tile, One) AS LessOne, 
                                    rf_local_multiply(tile, Two) AS TimesTwo, 
                                    rf_local_divide(tile, Two) AS OverTwo 
                                FROM r3""")

        ops.printSchema
        statsRow = ops.select(tile_mean(self.tileCol).alias('base'),
                           tile_mean("AndOne").alias('plus_one'),
                           tile_mean("LessOne").alias('minus_one'),
                           tile_mean("TimesTwo").alias('double'),
                           tile_mean("OverTwo").alias('half')) \
                        .first()

        self.assertTrue(_rounded_compare(statsRow.base, statsRow.plus_one - 1))
        self.assertTrue(_rounded_compare(statsRow.base, statsRow.minus_one + 1))
        self.assertTrue(_rounded_compare(statsRow.base, statsRow.double / 2))
        self.assertTrue(_rounded_compare(statsRow.base, statsRow.half * 2))

    def test_explode(self):
        import pyspark.sql.functions as F
        self.rf.select('spatial_key', explode_tiles(self.tileCol)).show()
        # +-----------+------------+---------+-------+
        # |spatial_key|column_index|row_index|tile   |
        # +-----------+------------+---------+-------+
        # |[2,1]      |4           |0        |10150.0|
        cell = self.rf.select(self.rf.spatialKeyColumn(), explode_tiles(self.rf.tile)) \
            .where(F.col("spatial_key.col")==2) \
            .where(F.col("spatial_key.row")==1) \
            .where(F.col("column_index")==4) \
            .where(F.col("row_index")==0) \
            .select(F.col("tile")) \
            .collect()[0][0]
        self.assertEqual(cell, 10150.0)

        # Test the sample version
        frac = 0.01
        sample_count = self.rf.select(explode_tiles_sample(frac, 1872, self.tileCol)).count()
        print('Sample count is {}'.format(sample_count))
        self.assertTrue(sample_count > 0)
        self.assertTrue(sample_count < (frac * 1.1) * 387000)  # give some wiggle room


    def test_maskByValue(self):
        from pyspark.sql.functions import lit

        # create an artificial mask for values > 25000; masking value will be 4
        mask_value = 4

        rf1 = self.rf.select(self.rf.tile,
                             local_multiply(
                                 convert_cell_type(
                                     local_greater_scalar_int(self.rf.tile, 25000),
                                     "uint8"),
                                  lit(mask_value)).alias('mask'))
        rf2 = rf1.select(rf1.tile, mask_by_value(rf1.tile, rf1.mask, lit(mask_value)).alias('masked'))
        result = rf2.agg(agg_no_data_cells(rf2.tile) < agg_no_data_cells(rf2.masked)) \
            .collect()[0][0]
        self.assertTrue(result)


    def test_resample(self):
        from pyspark.sql.functions import lit
        result = self.rf.select(
            tile_min(local_equal(
                resample(resample(self.rf.tile, lit(2)), lit(0.5)),
                self.rf.tile))
        ).collect()[0][0]

        self.assertTrue(result == 1)  # short hand for all values are true

    def test_exists_for_all(self):
        df = self.rf.withColumn('should_exist', tile_ones(5, 5, 'int8')) \
            .withColumn('should_not_exist', tile_zeros(5, 5, 'int8'))

        should_exist = df.select(exists(df.should_exist).alias('se')).take(1)[0].se
        should_any = df.select(any(df.should_exist).alias('se')).take(1)[0].se
        self.assertTrue(should_exist)
        self.assertTrue(should_any)

        should_not_exist = df.select(exists(df.should_not_exist).alias('se')).take(1)[0].se
        should_not_any = df.select(any(df.should_not_exist).alias('se')).take(1)[0].se
        self.assertTrue(not should_not_exist)
        self.assertTrue(not should_not_any)

        self.assertTrue(df.select(for_all(df.should_exist).alias('se')).take(1)[0].se)
        self.assertTrue(not df.select(for_all(df.should_not_exist).alias('se')).take(1)[0].se)


    def test_geomesa_pyspark(self):
        from pyspark.sql.functions import lit, udf, sum
        import shapely
        import pandas as pd
        import numpy.testing

        pandas_df = pd.DataFrame({
            'eye': ['a', 'b', 'c', 'd'],
            'x': [0.0, 1.0, 2.0, 3.0],
            'y': [-4.0, -3.0, -2.0, -1.0],
        })
        df = self.spark.createDataFrame(pandas_df)
        df = df.withColumn("point_geom",
                           st_point(df.x, df.y)
                           )
        df = df.withColumn("poly_geom", st_bufferPoint(df.point_geom, lit(1250.0)))

        # Use python shapely UDT in a UDF
        @udf("double")
        def area_fn(g):
            return g.area

        @udf("double")
        def length_fn(g):
            return g.length

        df = df.withColumn("poly_area", area_fn(df.poly_geom))
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
        xs_correct = pandas_df_out.point_geom.apply(lambda g: g.coords[0][0]) == pandas_df.x
        self.assertTrue(all(xs_correct))

        centroid_ys = pandas_df_out.poly_geom.apply(lambda g:
                                                      g.centroid.coords[0][1]).tolist()
        numpy.testing.assert_almost_equal(centroid_ys, pandas_df.y.tolist())

        # Including from UDF's
        numpy.testing.assert_almost_equal(
            pandas_df_out.poly_geom.apply(lambda g: g.area).values,
            pandas_df_out.poly_area.values
        )
        numpy.testing.assert_almost_equal(
            pandas_df_out.poly_geom.apply(lambda g: g.length).values,
            pandas_df_out.poly_len.values
        )


def suite():
    function_tests = unittest.TestSuite()
    return function_tests

unittest.TextTestRunner().run(suite())
