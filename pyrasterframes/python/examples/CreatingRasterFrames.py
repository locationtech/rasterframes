
#examples_setup
from . import resource_dir, example_session
#examples_setup

#py_crf_imports
from pyrasterframes import *
from pyspark.sql import *
#py_crf_imports

#py_crf_create_session
spark = example_session().withRasterFrames()
#py_crf_create_session

#py_crf_more_imports
# No additional imports needed for Python.
#py_crf_more_imports

#py_crf_create_rasterframe
rf = spark.read.geotiff(resource_dir.joinpath('L8-B8-Robinson-IL.tiff').as_uri())
rf.show(5, False)
#py_crf_create_rasterframe

#py_crf_metadata
rf.tileColumns()

rf.spatialKeyColumn()

rf.temporalKeyColumn()

rf.tileLayerMetadata()
#py_crf_metadata
