# Creating RasterFrames

There are a number of ways to create a `RasterFrame`, as enumerated in the sections below.

## Initialization

First, some standard `import`s:

```tut:silent
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark.io._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
```

Next, initialize the `SparkSession`, and call the `withRasterFrames` method on it:
 
```tut:silent
implicit val spark = SparkSession.builder().
  master("local").appName("RasterFrames").
  getOrCreate().
  withRasterFrames

import spark.implicits._
```

```tut:invisible
spark.sparkContext.setLogLevel("ERROR")
```

## From `ProjectedExtent`

The simplest mechanism for getting a RasterFrame is to use the `toRF(tileCols, tileRows)` extension method on `ProjectedRaster`. 

```tut
val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128)
rf.show(5, false)
```

## From `TileLayerRDD`

Another option is to use a GeoTrellis [`LayerReader`](https://docs.geotrellis.io/en/latest/guide/tile-backends.html), to get a `TileLayerRDD` for which there's also a `toRF` extension method. 

```scala
import geotrellis.spark._
val tiledLayer: TileLayerRDD[SpatialKey] = ???
val rf = tiledLayer.toRF
```

## Inspecting Structure

`RasterFrame` has a number of methods providing access to metadata about the contents of the RasterFrame. 

### Tile Column Names

```tut:book
rf.tileColumns.map(_.toString)
```

### Spatial Key Column Name

```tut:book
rf.spatialKeyColumn.toString
```

### Temporal Key Column

Returns an `Option[Column]` since not all RasterFrames have an explicit temporal dimension.

```tut:book
rf.temporalKeyColumn.map(_.toString)
```

### Tile Layer Metadata

The Tile Layer Metadata defines how the spatial/spatiotemporal domain is discretized into tiles, 
and what the key bounds are.

```tut
import spray.json._
// The `fold` is required because an `Either` is retured, depending on the key type. 
rf.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint
```

```tut:invisible
spark.stop()
```
