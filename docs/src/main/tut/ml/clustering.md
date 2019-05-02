# Clustering

In this example we will do some simple cell clustering based on multiband imagery.

## Setup 

First some setup:

```tut:silent
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.ml.TileExploder
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster._
import geotrellis.raster.render._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._

// Utility for reading imagery from our test data set
def readTiff(name: String): SinglebandGeoTiff = SinglebandGeoTiff(s"../core/src/test/resources/$name")

implicit val spark = SparkSession.builder().
  withKryoSerialization.
  master("local[*]").appName(getClass.getName).getOrCreate().withRasterFrames
spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._
```

## Loading Data

The first step is to load multiple bands of imagery and construct a single RasterFrame from them.

```tut:silent
val filenamePattern = "L8-B%d-Elkton-VA.tiff"
val bandNumbers = 1 to 4
val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray

// For each identified band, load the associated image file, convert to a RasterFrame, and join
val joinedRF = bandNumbers.
  map { b ⇒ (b, filenamePattern.format(b)) }.
  map { case (b, f) ⇒ (b, readTiff(f)) }.
  map { case (b, t) ⇒ t.projectedRaster.toRF(s"band_$b") }.
  reduce(_ spatialJoin _)
```

We should see a single `spatial_key` column along with 4 columns of tiles.

```tut
joinedRF.printSchema()
```

## ML Pipeline 

SparkML requires that each observation be in its own row, and those
observations be packed into a single `Vector`. The first step is to
"explode" the tiles into a single row per cell/pixel.

```tut:silent
val exploder = new TileExploder()
```

To "vectorize" the the band columns, as required by SparkML, we use the SparkML 
`VectorAssembler`. We then configure our algorithm, create the transformation pipeline,
and train our model. (Note: the selected value of *K* below is arbitrary.) 

```tut:silent
val assembler = new VectorAssembler().
  setInputCols(bandColNames).
  setOutputCol("features")

// Configure our clustering algorithm
val k = 5
val kmeans = new KMeans().setK(k)

// Combine the two stages
val pipeline = new Pipeline().setStages(Array(exploder, assembler, kmeans))

// Compute clusters
val model = pipeline.fit(joinedRF)
```

## Model Evaluation

At this point the model can be saved off for later use, or used immediately on the same
data we used to compute the model. First we run the data through the model to assign 
cluster IDs to each cell.

```tut
val clustered = model.transform(joinedRF)
clustered.show(8)
```

If we want to inspect the model statistics, the SparkML API requires us to go
through this unfortunate contortion:

```tut:silent
val clusterResults = model.stages.collect{ case km: KMeansModel ⇒ km}.head
```

Compute sum of squared distances of points to their nearest center:

```tut
val metric = clusterResults.computeCost(clustered)
println("Within set sum of squared errors: " + metric)
```

## Visualizing Results

The predictions are in a DataFrame with each row representing a separate pixel. 
To assemble a raster to visualize the cluster assignments, we have to go through a
multi-stage process to get the data back in tile form, and from there to combined
raster form.

First, we get the DataFrame back into RasterFrame form:

```tut:silent
val tlm = joinedRF.tileLayerMetadata.left.get

val retiled = clustered.groupBy($"spatial_key").agg(
  rf_assemble_tile(
    $"column_index", $"row_index", $"prediction",
    tlm.tileCols, tlm.tileRows, ByteConstantNoDataCellType
  )
)

val rf = retiled.asRF($"spatial_key", tlm)
```

To render our visualization, we convert to a raster first, and then use an
`IndexedColorMap` to assign each discrete cluster a different color, and finally
rendering to a PNG file.

```tut:silent
val raster = rf.toRaster($"prediction", 186, 169)

val clusterColors = IndexedColorMap.fromColorMap(
  ColorRamps.Viridis.toColorMap((0 until k).toArray)
)

raster.tile.renderPng(clusterColors).write("target/scala-2.11/tut/ml/clustered.png")
```

| Color Composite    | Cluster Assignments |
| ------------------ | ------------------- |
| ![](L8-RGB-VA.png) | ![](clustered.png)  |


```tut:invisible
spark.stop()
```
