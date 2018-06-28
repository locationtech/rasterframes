

#examples_setup
from . import resource_dir, example_session
#examples_setup

#py_nvdi_imports
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
from pyspark.sql.functions import udf
#py_nvdi_imports

#py_nvdi_create_session
spark = example_session().withRasterFrames()
#py_nvdi_create_session

#py_ndvi_create_rasterframe
redBand = spark.read.geotiff(resource_dir.joinpath('L8-B4-Elkton-VA.tiff').as_uri()).withColumnRenamed('tile', 'red_band')
nirBand = spark.read.geotiff(resource_dir.joinpath('L8-B5-Elkton-VA.tiff').as_uri()).withColumnRenamed('tile', 'nir_band')
#py_ndvi_create_rasterframe

#py_ndvi_column
rf = redBand.asRF().spatialJoin(nirBand.asRF()) \
    .withColumn("ndvi", normalizedDifference('red_band', 'nir_band')).asRF()
rf.printSchema()
rf.show(20)
#py_ndvi_column

spark.stop()