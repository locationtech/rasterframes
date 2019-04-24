# Raster Statistics

```tut:invisible
import org.locationtech.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark._
import geotrellis.spark.io._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().
  withKryoSerialization.
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
rf.select(rf.spatialKeyColumn, tile_dimensions($"tile")).show(3)
```

### Descriptive Statistics

#### NoData Counts

Count the numer of `NoData` and non-`NoData` cells in each tile.

```tut
rf.select(rf.spatialKeyColumn, no_data_cells($"tile"), data_cells($"tile")).show(3)
```

#### Tile Mean

Compute the mean value in each tile. Use `tileMean` for integral cell types, and `tileMeanDouble` for floating point
cell types.
 
```tut
rf.select(rf.spatialKeyColumn, tile_mean($"tile")).show(3)
```

#### Tile Summary Statistics

Compute a suite of summary statistics for each tile. Use `tile_stats` for integral cells types, and `tile_stats_double`
for floating point cell types.

```tut
rf.withColumn("stats", tile_stats($"tile")).select(rf.spatialKeyColumn, $"stats.*").show(3)
```

### Histogram

The `tile_histogram` function computes a histogram over the data in each tile. 

In this example we compute quantile breaks.

```tut
rf.select(tile_histogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
```

## Aggregate Statistics

The `agg_stats` function computes the same summary statistics as `tile_stats`, but aggregates them over the whole 
RasterFrame.

```tut
rf.select(agg_stats($"tile")).show()
```

A more involved example: extract bin counts from a computed `Histogram`.

```tut
rf.select(agg_approx_histogram($"tile")).
  map(h => for(v <- h.labels) yield(v, h.itemCount(v))).
  select(explode($"value") as "counts").
  select("counts._1", "counts._2").
  toDF("value", "count").
  orderBy(desc("count")).
  show(10)
```

```tut:invisible
spark.stop()
```

