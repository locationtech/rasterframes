
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
            .config("spark.kryo.registrator", "org.locationtech.rasterframes.util.RFKryoRegistrator")
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
        cls.rf = rf.withColumn('tile2', rf_convert_cell_type(cls.tileCol, 'float32')) \
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

    def test_tile_creation(self):
        base = self.spark.createDataFrame([1, 2, 3, 4], 'integer')
        tiles = base.select(rf_make_constant_tile(3, 3, 3, "int32"), rf_make_zeros_tile(3, 3, "int32"), rf_make_ones_tile(3, 3, "int32"))
        tiles.show()
        self.assertEqual(tiles.count(), 4)

    def test_tile_operations(self):
        df1 = self.rf.withColumnRenamed(self.tileCol, 't1').asRF()
        df2 = self.rf.withColumnRenamed(self.tileCol, 't2').asRF()
        df3 = df1.spatialJoin(df2).asRF()
        df3 = df3.withColumn('norm_diff', rf_normalized_difference('t1', 't2'))
        df3.printSchema()

        aggs = df3.agg(
            rf_agg_mean('norm_diff'),
        )
        aggs.show()
        row = aggs.first()

        self.assertTrue(_rounded_compare(row['rf_agg_mean(norm_diff)'], 0))


    def test_general(self):
        meta = self.rf.tileLayerMetadata()
        self.assertIsNotNone(meta['bounds'])
        df = self.rf.withColumn('dims',  rf_dimensions(self.tileCol)) \
            .withColumn('type', rf_cell_type(self.tileCol)) \
            .withColumn('dCells', rf_data_cells(self.tileCol)) \
            .withColumn('ndCells', rf_no_data_cells(self.tileCol)) \
            .withColumn('min', rf_tile_min(self.tileCol)) \
            .withColumn('max', rf_tile_max(self.tileCol)) \
            .withColumn('mean', rf_tile_mean(self.tileCol)) \
            .withColumn('sum', rf_tile_sum(self.tileCol)) \
            .withColumn('stats', rf_tile_stats(self.tileCol)) \
            .withColumn('extent', st_extent('geometry')) \
            .withColumn('extent_geom1', st_geometry('extent')) \
            .withColumn('ascii', rf_render_ascii(self.tileCol)) \
            .withColumn('log', rf_log(self.tileCol)) \
            .withColumn('exp', rf_exp(self.tileCol)) \
            .withColumn('expm1', rf_expm1(self.tileCol)) \
            .withColumn('round', rf_round(self.tileCol))
        # TODO: add test for rf_extent and rf_geometry once rastersource connector is integrated and we have
        #  a source of ProjectedRasterTiles.
        df.show()

    def test_rasterize(self):
        # NB: This test just makes sure rf_rasterize runs, not that the results are correct.
        withRaster = self.rf.withColumn('rasterized', rf_rasterize('geometry', 'geometry', lit(42), 10, 10))
        withRaster.show()

    def test_reproject(self):
        reprojected = self.rf.withColumn('reprojected', st_reproject('center', 'EPSG:4326', 'EPSG:3857'))
        reprojected.show()

    def test_aggregations(self):
        aggs = self.rf.agg(
            rf_agg_mean(self.tileCol),
            rf_agg_data_cells(self.tileCol),
            rf_agg_no_data_cells(self.tileCol),
            rf_agg_stats(self.tileCol),
            rf_agg_approx_histogram(self.tileCol)
        )
        aggs.show()
        row = aggs.first()

        self.assertTrue(_rounded_compare(row['rf_agg_mean(tile)'], 10160))
        print(row['rf_agg_data_cells(tile)'])
        self.assertEqual(row['rf_agg_data_cells(tile)'], 387000)
        self.assertEqual(row['rf_agg_no_data_cells(tile)'], 1000)
        self.assertEqual(row['rf_agg_stats(tile)'].data_cells, row['rf_agg_data_cells(tile)'])


    def test_sql(self):

        self.rf.createOrReplaceTempView("rf")

        dims = self.rf.withColumn('dims',  rf_dimensions(self.tileCol)).first().dims
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
        statsRow = ops.select(rf_tile_mean(self.tileCol).alias('base'),
                           rf_tile_mean("AndOne").alias('plus_one'),
                           rf_tile_mean("LessOne").alias('minus_one'),
                           rf_tile_mean("TimesTwo").alias('double'),
                           rf_tile_mean("OverTwo").alias('half')) \
                        .first()

        self.assertTrue(_rounded_compare(statsRow.base, statsRow.plus_one - 1))
        self.assertTrue(_rounded_compare(statsRow.base, statsRow.minus_one + 1))
        self.assertTrue(_rounded_compare(statsRow.base, statsRow.double / 2))
        self.assertTrue(_rounded_compare(statsRow.base, statsRow.half * 2))

    def test_explode(self):
        import pyspark.sql.functions as F
        self.rf.select('spatial_key', rf_explode_tiles(self.tileCol)).show()
        # +-----------+------------+---------+-------+
        # |spatial_key|column_index|row_index|tile   |
        # +-----------+------------+---------+-------+
        # |[2,1]      |4           |0        |10150.0|
        cell = self.rf.select(self.rf.spatialKeyColumn(), rf_explode_tiles(self.rf.tile)) \
            .where(F.col("spatial_key.col")==2) \
            .where(F.col("spatial_key.row")==1) \
            .where(F.col("column_index")==4) \
            .where(F.col("row_index")==0) \
            .select(F.col("tile")) \
            .collect()[0][0]
        self.assertEqual(cell, 10150.0)

        # Test the sample version
        frac = 0.01
        sample_count = self.rf.select(rf_explode_tiles_sample(frac, 1872, self.tileCol)).count()
        print('Sample count is {}'.format(sample_count))
        self.assertTrue(sample_count > 0)
        self.assertTrue(sample_count < (frac * 1.1) * 387000)  # give some wiggle room


    def test_maskByValue(self):
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


    def test_raster_source_reader(self):
        import pandas as pd
        # much the same as RasterSourceDataSourceSpec here; but using https PDS. Takes about 30s to run

        def l8path(b):
            assert b in range(1, 12)
            base = "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/199/026/LC08_L1TP_199026_20180919_20180928_01_T1/LC08_L1TP_199026_20180919_20180928_01_T1_B{}.TIF"
            return base.format(b)

        path_param = '\n'.join([l8path(b) for b in [1, 2, 3]])  # "http://foo.com/file1.tif,http://foo.com/file2.tif"
        tile_size = 512
        df = self.spark.read.format('rastersource') \
                            .options(paths=path_param, tileDimensions='{},{}'.format(tile_size, tile_size)) \
                            .load()

        # schema is tile_path and tile
        df.printSchema()
        self.assertTrue(len(df.columns) == 2 and 'tile_path' in df.columns and 'tile' in df.columns)

        # the most common tile dimensions should be as passed to `options`, showing that options are correctly applied
        tile_size_df = df.select(rf_dimensions(df.tile).rows.alias('r'), rf_dimensions(df.tile).cols.alias('c'))\
            .groupby(['r', 'c']).count().toPandas()
        most_common_size = tile_size_df.loc[tile_size_df['count'].idxmax()]
        self.assertTrue(most_common_size.r == tile_size and most_common_size.c == tile_size)

        # all rows are from a single source URI
        path_count = df.groupby(df.tile_path).count()
        print(path_count.toPandas())
        self.assertTrue(path_count.count() == 3)

        ###  Similar to the scala side's `fromTable`, read from a table with columns giving URI paths

        scene_dict = {
            1: 'http://landsat-pds.s3.amazonaws.com/c1/L8/015/041/LC08_L1TP_015041_20190305_20190309_01_T1/LC08_L1TP_015041_20190305_20190309_01_T1_B{}.TIF',
            2: 'http://landsat-pds.s3.amazonaws.com/c1/L8/015/042/LC08_L1TP_015042_20190305_20190309_01_T1/LC08_L1TP_015042_20190305_20190309_01_T1_B{}.TIF',
            3: 'http://landsat-pds.s3.amazonaws.com/c1/L8/016/041/LC08_L1TP_016041_20190224_20190309_01_T1/LC08_L1TP_016041_20190224_20190309_01_T1_B{}.TIF',
        }

        def path(scene, band):
            assert band in range(1, 12)
            p = scene_dict[scene]
            return p.format(band)

        path_table_hive_name = 'path_table'
        # Create a pandas dataframe (makes it easy to create spark df)
        path_pandas = pd.DataFrame([
            {'b1': path(1, 1), 'b2': path(1, 2), 'b3': path(1, 3)},
            {'b1': path(2, 1), 'b2': path(2, 2), 'b3': path(2, 3)},
            {'b1': path(3, 1), 'b2': path(3, 2), 'b3': path(3, 3)},
        ])
        # comma separated list of column names containing URI's to read.
        csv_columns = ','.join(path_pandas.columns.tolist())  # 'b1,b2,b3'
        path_table = self.spark.createDataFrame(path_pandas)
        path_table.createOrReplaceTempView(path_table_hive_name)

        path_df = self.spark.read.format('rastersource') \
                            .options(pathTable=path_table_hive_name, pathTableColumns=csv_columns,
                                     tileDimensions='512,512') \
                            .load()

        path_df.printSchema()
        self.assertTrue(len(path_df.columns) == 6)  # three bands times {path, tile}
        self.assertTrue(path_df.select('b1_path').distinct().count() == 3)  # as per scene_dict
        b1_paths_maybe = path_df.select('b1_path').distinct().collect()
        b1_paths = [s.format('1') for s in scene_dict.values()]
        self.assertTrue(all([row.b1_path in b1_paths for row in b1_paths_maybe]))


def suite():
    function_tests = unittest.TestSuite()
    return function_tests

unittest.TextTestRunner().run(suite())
