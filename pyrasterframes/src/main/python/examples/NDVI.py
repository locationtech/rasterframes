from . import test_resource_dir

import pyrasterframes
from pyrasterframes.rasterfunctions import rf_normalized_difference

import os

spark = pyrasterframes.get_spark_session()

red_path = os.path.join(test_resource_dir(), 'L8-B4-Elkton-VA.tiff')
nir_path = os.path.join(test_resource_dir(), 'L8-B5-Elkton-VA.tiff')

red_band = spark.read.geotiff(red_path).withColumnRenamed('tile', 'red_band')
nir_band = spark.read.geotiff(nir_path).withColumnRenamed('tile', 'nir_band')

rf = red_band.asRF().spatialJoin(nir_band.asRF()) \
    .withColumn("ndvi", rf_normalized_difference('red_band', 'nir_band'))
rf.printSchema()
rf.show(20)

spark.stop()