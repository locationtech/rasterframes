# Exporting&nbsp;RasterFrames

```tut:invisible
import astraea.spark.rasterframes._
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().config("spark.ui.enabled", "false").
  master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
val scene = SinglebandGeoTiff("../core/src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128).cache()
```

While the goal of RasterFrames is to make it as easy as possible to do your geospatial analysis with a single 
construct, it is helpful to be able to transform it into other representations for various use cases.

## Converting to Array

The cell values within a `Tile` are encoded internally as an array. There may be use cases 
where the additional context provided by the `Tile` construct is no longer needed and one would
prefer to work with the underlying array data.

The @scaladoc[`tileToArray`][tileToArray] column function requires a type parameter to indicate the array element
type you would like used. The following types may be used: `Int`, `Double`, `Byte`, `Short`, `Float`

```tut
val withArrays = rf.withColumn("tileData", tileToArray[Short]($"tile")).drop("tile")
withArrays.show(5, 40)
```

You can convert the data back to an array, but you have to specify the target tile dimensions. 

```tut
val tileBack = withArrays.withColumn("tileAgain", arrayToTile($"tileData", 128, 128))
tileBack.drop("tileData").show(5, 40)
``` 

Note that the created tile will not have a `NoData` value associated with it. Here's how you can do that:

```tut
val tileBackAgain = withArrays.withColumn("tileAgain", withNoData(arrayToTile($"tileData", 128, 128), 3))
tileBackAgain.drop("tileData").show(5, 50)
```

## Writing to Parquet

It is often useful to write Spark results in a form that is easily reloaded for subsequent analysis. 
The [Parquet](https://parquet.apache.org/)columnar storage format, native to Spark, is ideal for this. RasterFrames
work just like any other DataFrame in this scenario as long as @scaladoc[`rfInit`][rfInit] is called to register
the imagery types.


Let's assume we have a RasterFrame we've done some fancy processing on: 

```tut:silent
import geotrellis.raster.equalization._
val equalizer = udf((t: Tile) => t.equalize())
val equalized = rf.withColumn("equalized", equalizer($"tile")).asRF
```

```tut
equalized.printSchema
equalized.select(aggStats($"tile")).show(false)
equalized.select(aggStats($"equalized")).show(false)
```

We write it out just like any other DataFrame, including the ability to specify partitioning:

```tut:silent
val filePath = "/tmp/equalized.parquet"
equalized.select("*", "spatial_key.*").write.partitionBy("col", "row").mode(SaveMode.Overwrite).parquet(filePath)
```

Let's confirm partitioning happened as expected:

```tut
import java.io.File
new File(filePath).list.filter(f => !f.contains("_"))
```

Now we can load the data back in and check it out:

```tut:silent
val rf2 = spark.read.parquet(filePath)
```

```tut
rf2.printSchema
equalized.select(aggStats($"tile")).show(false)
equalized.select(aggStats($"equalized")).show(false)
```


## Exporting a Raster

For the purposes of debugging, the RasterFrame tiles can be reassembled back into a raster for viewing. However, 
keep in mind that this will download all the data to the driver, and reassemble it in-memory. So it's not appropriate 
for very large coverages.

Here's how one might render the image to a georeferenced GeoTIFF file: 

```tut:silent
import geotrellis.raster.io.geotiff.GeoTiff
val image = equalized.toRaster($"equalized", 774, 500)
GeoTiff(image).write("target/scala-2.11/tut/rf-raster.tiff")
```

[*Download GeoTIFF*](rf-raster.tiff)

Here's how one might render a raster frame to a false color PNG file:

```tut:silent
val colors = ColorMap.fromQuantileBreaks(image.tile.histogram, ColorRamps.BlueToOrange)
image.tile.color(colors).renderPng().write("target/scala-2.11/tut/rf-raster.png")
```

![](rf-raster.png)

## Exporting to a GeoTrellis Layer

For future analysis it is helpful to persist a RasterFrame as a [GeoTrellis layer](http://geotrellis.readthedocs.io/en/latest/guide/tile-backends.html).

First, convert the RasterFrame into a TileLayerRDD. The return type is an Either;
the `left` side is for spatial-only keyed data

```tut:book
val tlRDD = equalized.toTileLayerRDD($"equalized").left.get
```

Then create a GeoTrellis layer writer:

```tut:silent
import java.nio.file.Files
import spray.json._
import DefaultJsonProtocol._
import geotrellis.spark.io._
val p = Files.createTempDirectory("gt-store")
val writer: LayerWriter[LayerId] = LayerWriter(p.toUri)

val layerId = LayerId("equalized", 0)
writer.write(layerId, tlRDD, index.ZCurveKeyIndexMethod)
```

Take a look at the metadata in JSON format:
```tut
AttributeStore(p.toUri).readMetadata[JsValue](layerId).prettyPrint
```

## Converting to `RDD` and `TileLayerRDD`

Since a `RasterFrame` is just a `DataFrame` with extra metadata, the method 
@scaladoc[`DataFrame.rdd`][rdd] is available for simple conversion back to `RDD` space. The type returned 
by `.rdd` is dependent upon how you select it.


```tut
import scala.reflect.runtime.universe._
def showType[T: TypeTag](t: T) = println(implicitly[TypeTag[T]].tpe.toString)

showType(rf.rdd)

showType(rf.select(rf.spatialKeyColumn, $"tile".as[Tile]).rdd) 

showType(rf.select(rf.spatialKeyColumn, $"tile").as[(SpatialKey, Tile)].rdd) 
```

If your goal convert a single tile column with its spatial key back to a `TileLayerRDD[K]`, then there's an additional
extension method on `RasterFrame` called [`toTileLayerRDD`][toTileLayerRDD], which preserves the tile layer metadata,
enhancing interoperation with GeoTrellis RDD extension methods.

```tut
showType(rf.toTileLayerRDD($"tile".as[Tile]))
```

```tut:invisible
spark.stop()
```

[rfInit]: astraea.spark.rasterframes.package#rfInit%28SQLContext%29:Unit
[rdd]: org.apache.spark.sql.Dataset#frdd:org.apache.spark.rdd.RDD[T]
[toTileLayerRDD]: astraea.spark.rasterframes.RasterFrameMethods#toTileLayerRDD%28tileCol:RasterFrameMethods.this.TileColumn%29:Either[geotrellis.spark.TileLayerRDD[geotrellis.spark.SpatialKey],geotrellis.spark.TileLayerRDD[geotrellis.spark.SpaceTimeKey]]
[tileToArray]: astraea.spark.rasterframes.ColumnFunctions#tileToArray

