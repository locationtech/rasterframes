from . import test_resource_dir
from pyrasterframes.utils import create_rf_spark_session
from pyrasterframes import *
from pyspark.sql import *
import os

spark = create_rf_spark_session().withRasterFrames()

rf = spark.read.geotiff(os.path.join(test_resource_dir(), 'L8-B8-Robinson-IL.tiff'))
rf.show(5, False)

rf.tileColumns()

rf.spatialKeyColumn()

rf.temporalKeyColumn()

rf.tileLayerMetadata()
