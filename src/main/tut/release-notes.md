# Release Notes
## 0.5.11

* Significant performance improvement in `explodeTiles` (1-2 orders of magnitude). See [#38](https://github.com/s22s/raster-frames/issues/38)
* Fixed bugs in `NoData` handling when converting to `Double` tiles.

## 0.5.12

* Added `withSpatialIndex` to introduce a column assigning a z-curve index value based on the tile's centroid in EPSG:4326. 
* Added column-appending convenience methods: `withExtent`, `withCenter`,  `withCenterLatLng`
* Documented example of creating a GeoTrellis layer from a RasterFrame.
* Added Spark 2.2.0 forward-compatibility
* Upgraded to GeoTrellis 1.2.0-RC2

## 0.5.11

* Significant performance improvement in `explodeTiles` (1-2 orders of magnitude). See [#38](https://github.com/s22s/raster-frames/issues/38)
* Fixed bugs in `NoData` handling when converting to `Double` tiles.

## 0.5.10

* Upgraded to shapeless 2.3.2
* Fixed [#36](https://github.com/s22s/raster-frames/issues/36), [#37](https://github.com/s22s/raster-frames/issues/37)

## 0.5.9

* Ported to sbt 1.0.3
* Added sbt-generated `astraea.spark.rasterframes.RFBuildInfo`
* Fixed bug in computing `aggMean` when one or more tiles are `null` 
* Deprecated `rfIinit` in favor of `SparkSession.withRasterFrames` or `SQLContext.withRasterFrames` extension methods

## 0.5.8

* Upgraded to GeoTrellis 1.2.0-RC1
* Added [`REPLsent`-based](https://github.com/marconilanna/REPLesent) tour of RasterFrames
* Moved Giter8 template to separate repository `s22s/raster-frames.g8` due to sbt limitations
* Updated _Getting Started_ to reference new Giter8 repo
* Changed SQL function name `rf_stats` and `rf_histogram` to `rf_aggStats` and `rf_aggHistogram` 
  for consistency with DataFrames API

## 0.5.7

* Created faster implementation of aggregate statistics.
* Fixed bug in deserialization of `TileUDT`s originating from `ConstantTile`s
* Fixed bug in serialization of `NoDataFilter` within SparkML pipeline
* Refactoring of UDF organization
* Various documentation tweaks and updates
* Added Giter8 template

## 0.5.6

* `TileUDF`s are encoded using directly into Catalyst--without Kryo--resulting in an insane
 decrease in serialization time for small tiles (`int8`, <= 128²), and pretty awesome speedup for
 all other cell types other than `float32` (marginal slowing). While not measured, memory 
 footprint is expected to have gone down.


## 0.5.5

* `aggStats` and `tileMean` functions rewritten to compute simple statistics directly rather than using `StreamingHistogram`
* `tileHistogramDouble` and `tileStatsDouble` were replaced by `tileHistogram` and `tileStats`
* Added `tileSum`, `tileMin` and `tileMax` functions 
* Added `aggMean`, `aggDataCells` and `aggNoDataCells` aggregate functions.
* Added `localAggDataCells` and `localAggNoDataCells` cell-local (tile generating) fuctions
* Added `tileToArray` and `arrayToTile`
* Overflow fix in `LocalStatsAggregateFunction`
