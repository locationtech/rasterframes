# Creating RasterFrames

## Initialization

There are a couple of setup steps necessary anytime you want to work with RasterFrames. the first is to import the API symbols into scope:



```tut:silent
import astraea.spark.rasterframes._
import org.apache.spark.sql._
```


Next, initialize the `SparkSession`, and call the `withRasterFrames` method on it:


```tut:silent
implicit val spark = SparkSession.builder().
  master("local").appName("RasterFrames").
  config("spark.ui.enabled", "false").
  getOrCreate().
  withRasterFrames
```

And, as is standard Spark SQL practice, we import additional DataFrame support:

```tut:silent
import spark.implicits._
```

```tut:invisible
spark.sparkContext.setLogLevel("ERROR")
```

Now we are ready to create a RasterFrame.

## Reading a GeoTIFF

The most straightforward way to create a `RasterFrame` is to read a [GeoTIFF](https://en.wikipedia.org/wiki/GeoTIFF)
file using a RasterFrame [`DataSource`](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
designed for this purpose.

First add the following import:


```tut:silent
import astraea.spark.rasterframes.datasource.geotiff._
import java.io.File
```

(This is what adds the `.geotiff` method to `spark.read` below.)

Then we use the `DataFrameReader` provided by `spark.read` to read the GeoTIFF:


```tut:book
val samplePath = new File("../core/src/test/resources/LC08_RGB_Norfolk_COG.tiff")
val tiffRF = spark.read.
  geotiff.
  loadRF(samplePath.toURI)
```


Let's inspect the structure of what we get back:


```tut
tiffRF.printSchema()
```

As reported by Spark, RasterFrames extracts 6 columns from the GeoTIFF we selected. Some of these columns are dependent
on the contents of the source data, and some are are always available. Let's take a look at these in more detail.

* `spatial_key`: GeoTrellis assigns a `SpatialKey` or a `SpaceTimeKey` to each tile, mapping it to the layer grid from
which it came. If it has a `SpaceTimeKey`, RasterFrames will split it into a `SpatialKey` and a `TemporalKey` (the
latter with column name `temporal_key`).
* `extent`: The bounding box of the tile in the tile's native CRS.
* `metadata`: The TIFF format header tags found in the file.
* `tile` or `tile_n` (where `n` is a band number): For singleband GeoTIFF files, the `tile` column contains the cell
data split into tiles. For multiband tiles, each column with `tile_` prefix contains each of the sources bands,
in the order they were stored.

See the section [Inspecting a `RasterFrame`](#inspecting-a--code-rasterframe--code-) (below) for more details on accessing the RasterFrame's metadata.

## Reading a GeoTrellis Layer

If your imagery is already ingested into a [GeoTrellis layer](https://docs.geotrellis.io/en/latest/guide/spark.html#writing-layers),
you can use the RasterFrames GeoTrellis DataSource. There are two parts to this GeoTrellis Layer support. The first
is the GeoTrellis Catalog DataSource, which lists the GeoTrellis layers available at a URI. The second part is the actual
RasterFrame reader for pulling a layer into a RasterFrame.

Before we show how all of this works we need to have a GeoTrellis layer to work with. We can create one with the RasterFrame we constructed above.


```tut:silent
import astraea.spark.rasterframes.datasource.geotrellis._
import java.nio.file.Files

val base = Files.createTempDirectory("rf-").toUri
val layer = Layer(base, "sample", 0)
tiffRF.write.geotrellis.asLayer(layer).save()
```

Now we can point our catalog reader at the base directory and see what was saved:

```tut
val cat = spark.read.geotrellisCatalog(base)
cat.printSchema
cat.show()
```

As you can see, there's a lot of information stored in each row of the catalog. Most of this is associated with how the
layer is discretized. However, there may be other application specific metadata serialized with a layer that can be use
to filter the catalog entries or select a specific one. But for now, we're just going to load a RasterFrame in from the
catalog using a convenience function.

```tut
val firstLayer = cat.select(geotrellis_layer).first
val rfAgain = spark.read.geotrellis.loadRF(firstLayer)
rfAgain.show()
```

If you already know the `LayerId` of what you're wanting to read, you can bypass working with the catalog:

```tut
val anotherRF = spark.read.geotrellis.loadRF(layer)
```

## Writing a GeoTrellis Layer

**TODO**

## Using GeoTrellis APIs

If you are used to working directly with the GeoTrellis APIs, there are a number of additional ways to create a `RasterFrame`, as enumerated in the sections below.

First, some more `import`s:

```tut:silent
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark.io._
```


### From `ProjectedExtent`

The simplest mechanism for getting a RasterFrame is to use the `toRF(tileCols, tileRows)` extension method on `ProjectedRaster`.


```tut
val scene = SinglebandGeoTiff("../core/src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128)
rf.show(5, false)
```


### From `TileLayerRDD`

Another option is to use a GeoTrellis [`LayerReader`](https://docs.geotrellis.io/en/latest/guide/tile-backends.html),
to get a `TileLayerRDD` for which there's also a `toRF` extension method.


```scala
import geotrellis.spark._
val tiledLayer: TileLayerRDD[SpatialKey] = ???
val rf = tiledLayer.toRF
```


## Inspecting a `RasterFrame`

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

The Tile Layer Metadata defines how the spatial/spatiotemporal domain is discretized into tiles, and what the key bounds are.

```tut
import spray.json._
// NB: The `fold` is required because an `Either` is returned, depending on the key type.
rf.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint
```

```tut:invisible
spark.stop()
```

