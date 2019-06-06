from . import resource_dir, example_session
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
import os

spark = example_session().withRasterFrames()

redBand = spark.read.geotiff(os.path.join(resource_dir, 'L8-B4-Elkton-VA.tiff')).withColumnRenamed('tile', 'red_band')
nirBand = spark.read.geotiff(os.path.join(resource_dir, 'L8-B5-Elkton-VA.tiff')).withColumnRenamed('tile', 'nir_band')

rf = redBand.asRF().spatialJoin(nirBand.asRF()) \
    .withColumn("ndvi", rf_normalized_difference('red_band', 'nir_band'))
rf.printSchema()
rf.show(20)

spark.stop()