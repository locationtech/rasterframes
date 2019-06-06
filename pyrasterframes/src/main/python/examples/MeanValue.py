from examples import resource_dir, example_session
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
import os

spark = example_session().withRasterFrames()

rf = spark.read.geotiff(os.path.join(resource_dir, 'L8-B8-Robinson-IL.tiff'))
rf.show(5, False)

rf.agg(rf_agg_no_data_cells('tile'), rf_agg_data_cells('tile'), rf_agg_mean('tile')).show(5, False)
