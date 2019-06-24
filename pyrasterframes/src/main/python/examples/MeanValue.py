from . import test_resource_dir

import pyrasterframes
from pyrasterframes.rasterfunctions import rf_agg_no_data_cells, rf_agg_data_cells, rf_agg_mean

import os

spark = pyrasterframes.get_spark_session()

rf = spark.read.geotiff(os.path.join(test_resource_dir(), 'L8-B8-Robinson-IL.tiff'))
rf.show(5, False)

rf.agg(rf_agg_no_data_cells('tile'), rf_agg_data_cells('tile'), rf_agg_mean('tile')).show(5, False)
