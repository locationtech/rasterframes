from . import test_resource_dir
import pyrasterframes
import os

spark = pyrasterframes.get_spark_session()

rf = spark.read.geotiff(os.path.join(test_resource_dir(), 'L8-B8-Robinson-IL.tiff'))
rf.show(5, False)

rf.tileColumns()

rf.spatialKeyColumn()

rf.temporalKeyColumn()

rf.tileLayerMetadata()
