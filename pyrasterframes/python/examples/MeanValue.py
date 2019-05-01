
#examples_setup
from . import resource_dir, example_session
#examples_setup


#py_mv_imports
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
#py_mv_imports

#py_mv_create_session
spark = example_session().withRasterFrames()
#py_mv_create_session

#py_mv_create_rasterframe
rf = spark.read.geotiff(resource_dir.joinpath('L8-B8-Robinson-IL.tiff').as_uri())
rf.show(5, False)
#py_mv_create_rasterframe

#py_mv_find_mean
tileCol = 'tile'
rf.agg(rf_agg_no_data_cells(tileCol), rf_agg_data_cells(tileCol), rf_agg_mean(tileCol)).show(5, False)
#py_mv_find_mean