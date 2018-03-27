# Spatial Queries

An important aspect of working with big geospatial data is the ability to filter based on spatio-temporal predicates. RasterFrames uses [GeoMesa Spark JTS](http://www.geomesa.org/documentation/current/user/spark/spark_jts.html) for specifying and executing spatial queries against RasterFrame data sources.

@@@ note
Until this section is written, please see [the `GeoTrellisDataSource` tests](https://github.com/s22s/raster-frames/blob/develop/datasource/src/test/scala/astraea/spark/rasterframes/datasource/geotrellis/GeoTrellisDataSourceSpec.scala) for examples on how to use the spatial query support.
@@@
