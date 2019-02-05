# Computing NDVI

```tut:invisible
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark._
import geotrellis.spark.io._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().
  config("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName).
  master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
val scene = SinglebandGeoTiff("../core/src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128).cache()
```

Here's an example of computing the Normalized Differential Vegetation Index (NDVI) is a 
standardized vegetation index which allows us to generate an image highlighting differences in
relative biomass. 

> “An NDVI is often used worldwide to monitor drought, monitor and predict agricultural production, assist in predicting hazardous fire zones, and map desert encroachment. The NDVI is preferred for global vegetation monitoring because it helps to compensate for changing illumination conditions, surface slope, aspect, and other extraneous factors” (Lillesand. *Remote sensing and image interpretation*. 2004).

```tut:silent
def redBand = SinglebandGeoTiff("../core/src/test/resources/L8-B4-Elkton-VA.tiff").projectedRaster.toRF("red_band")
def nirBand = SinglebandGeoTiff("../core/src/test/resources/L8-B5-Elkton-VA.tiff").projectedRaster.toRF("nir_band")

// Define UDF for computing NDVI from red and NIR bands
val ndvi = udf((red: Tile, nir: Tile) ⇒ {
  val redd = red.convert(DoubleConstantNoDataCellType)
  val nird = nir.convert(DoubleConstantNoDataCellType)
  (nird - redd)/(nird + redd)
})

// We use `asRF` to indicate we know the structure still conforms to RasterFrame constraints
val rf = redBand.spatialJoin(nirBand).withColumn("ndvi", ndvi($"red_band", $"nir_band")).asRF

val pr = rf.toRaster($"ndvi", 466, 428)

val brownToGreen = ColorRamp(
  RGBA(166,97,26,255),
  RGBA(223,194,125,255),
  RGBA(245,245,245,255),
  RGBA(128,205,193,255),
  RGBA(1,133,113,255)
).stops(128)

val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)
pr.tile.color(colors).renderPng().write("target/scala-2.11/tut/apps/rf-ndvi.png")
```

![](rf-ndvi.png)

For a georefrenced singleband greyscale image, we could have done this instead: 

```scala
GeoTiff(pr).write("ndvi.tiff")
```


```tut:invisible
spark.stop()
```

