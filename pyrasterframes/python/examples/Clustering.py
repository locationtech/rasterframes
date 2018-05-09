
#examples_setup
from examples import resource_dir
#examples_setup

#py_cl_imports
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
#py_cl_imports

#py_cl_create_session
spark = SparkSession.builder. \
    master("local[*]"). \
    appName("RasterFrames"). \
    config("spark.ui.enabled", "false"). \
    getOrCreate(). \
    withRasterFrames()
#py_cl_create_session

# The first step is to load multiple bands of imagery and construct
# a single RasterFrame from them.
filenamePattern = "L8-B{}-Elkton-VA.tiff"
bandNumbers = range(1, 8)
bandColNames = list(map(lambda n: 'band_{}'.format(n), bandNumbers))

# For each identified band, load the associated image file
from functools import reduce
joinedRF = reduce(lambda rf1, rf2: rf1.asRF().spatialJoin(rf2.drop('bounds').drop('metadata')),
                  map(lambda bf: spark.read.geotiff(bf[1]) \
                      .withColumnRenamed('tile', 'band_{}'.format(bf[0])),
                  map(lambda b: (b, resource_dir.joinpath(filenamePattern.format(b)).as_uri()), bandNumbers)))

# We should see a single spatial_key column along with columns of tiles.
joinedRF.printSchema()


# SparkML requires that each observation be in its own row, and those
# observations be packed into a single `Vector`. The first step is to
# "explode" the tiles into a single row per cell/pixel
exploder = TileExploder()


# To "vectorize" the the band columns we use the SparkML `VectorAssembler`
assembler = VectorAssembler() \
    .setInputCols(bandColNames) \
    .setOutputCol("features")

# Configure our clustering algorithm
k = 5
kmeans = KMeans().setK(k)

# Combine the two stages
pipeline = Pipeline().setStages([exploder, assembler, kmeans])


# Compute clusters
model = pipeline.fit(joinedRF)

# Run the data through the model to assign cluster IDs to each
clustered = model.transform(joinedRF)
clustered.show(8)


# If we want to inspect the model statistics, the SparkML API requires us to go
# through this unfortunate contortion:
clusterResults = list(filter(lambda x: str(x).startswith('KMeans'), model.stages))[0]

# Compute sum of squared distances of points to their nearest center
metric = clusterResults.computeCost(clustered)
print("Within set sum of squared errors: %s" % metric)

tlm = joinedRF.tileLayerMetadata()
layout = tlm['layoutDefinition']['tileLayout']

retiled = clustered.groupBy('spatial_key').agg(
    assembleTile('column_index', 'row_index', 'prediction',
        layout['tileCols'], layout['tileRows'], 'int8')
)

rf = retiled.asRF('spatial_key', tlm)
rf.printSchema()
rf.show()

spark.stop()