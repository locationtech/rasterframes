# Raster Statistics

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
   master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
val scene = SinglebandGeoTiff("../core/src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128).cache()
```

RasterFrames has a number of extension methods and columnar functions for performing analysis on tiles.

## Tile Statistics 

### Tile Dimensions

Get the nominal tile dimensions. Depending on the tiling there may be some tiles with different sizes on the edges.

```tut
rf.select(rf.spatialKeyColumn, tileDimensions($"tile")).show(3)
```

### Descriptive Statistics

#### NoData Counts

Count the numer of `NoData` and non-`NoData` cells in each tile.

```tut
rf.select(rf.spatialKeyColumn, noDataCells($"tile"), dataCells($"tile")).show(3)
```

#### Tile Mean

Compute the mean value in each tile. Use `tileMean` for integral cell types, and `tileMeanDouble` for floating point
cell types.
 
```tut
rf.select(rf.spatialKeyColumn, tileMean($"tile")).show(3)
```

#### Tile Summary Statistics

Compute a suite of summary statistics for each tile. Use `tileStats` for integral cells types, and `tileStatsDouble`
for floating point cell types.

```tut
rf.withColumn("stats", tileStats($"tile")).select(rf.spatialKeyColumn, $"stats.*").show(3)
```

### Histogram

The `tileHistogram` function computes a histogram over the data in each tile. See the 
@scaladoc[GeoTrellis `Histogram`](geotrellis.raster.histogram.Histogram) documentation for details on what's
available in the resulting data structure. Use this version for integral cell types, and `tileHistorgramDouble` for
floating  point cells types. 

In this example we compute quantile breaks.

```tut
rf.select(tileHistogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
```

## Aggregate Statistics

The `aggStats` function computes the same summary statistics as `tileStats`, but aggregates them over the whole 
RasterFrame.

```tut
rf.select(aggStats($"tile")).show()
```

A more involved example: extract bin counts from a computed `Histogram`.

```tut
rf.select(aggHistogram($"tile")).
  map(h => for(v <- h.values) yield(v, h.itemCount(v))).
  select(explode($"value") as "counts").
  select("counts._1", "counts._2").
  toDF("value", "count").
  orderBy(desc("count")).
  show(10)
```

```tut:invisible
spark.stop()
```

