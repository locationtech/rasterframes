# RasterFrames

_RasterFrames™_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer 
operations of [GeoTrellis](https://geotrellis.io/). 

The source code can be found on GitHub at [locationtech/rasterframes](https://github.com/locationtech/rasterframes).

The underlying purpose of RasterFrames™ is to allow data scientists and software developers to process
and analyze geospatial-temporal raster data with the same flexibility and ease as any other Spark Catalyst data type. At its
core is a user-defined type (UDT) called @scaladoc[`TileUDT`](org.apache.spark.sql.gt.types.TileUDT), 
which encodes a GeoTrellis @scaladoc[`Tile`](geotrellis.raster.Tile) in a form the Spark Catalyst engine can process. 
Furthermore, we extend the definition of a DataFrame to encompass some additional invariants, allowing for geospatial 
operations within and between RasterFrames to occur, while still maintaining necessary geo-referencing constructs.

To learn more, please see the @ref:[Getting Started](getting-started.md) section of this manual.

<img src="RasterFramePipeline.svg" width="600px"/>

@@@ note
RasterFrames™ is a new project under active development. Feedback and contributions are welcomed as we look to improve it. 
Please [submit an issue](https://github.com/locationtech/rasterframes/issues) if there's a particular feature you think should be included.
@@@

@@@ div { .md-left}

## Detailed Contents

@@ toc { depth=2 }

@@@

@@@ div { .md-right }

## Related Links

* [Gitter Channel](https://gitter.im/s22s/raster-frames)&nbsp;&nbsp;[![Join the chat at https://gitter.im/s22s/raster-frames](https://badges.gitter.im/s22s/raster-frames.svg)](https://gitter.im/s22s/raster-frames?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* [API Documentation](latest/api/index.html)
* [GitHub Repository](https://github.com/locationtech/rasterframes)
* [GeoTrellis Documentation](https://docs.geotrellis.io/en/latest/)
* [Astraea, Inc.](http://www.astraea.earth/) (the company behind RasterFrames)

@@@

@@@ div { .md-clear }

&nbsp;

@@@

@@@ index
* [Getting Started](getting-started.md)
* [PyRasterFrames](pyrasterframes.md)
* [Creating RasterFrames](creating-rasterframes.md)
* [Spatial Queries](spatial-queries.md)
* [Applications](apps/index.md)
* [Machine Learning](ml/index.md)
* [Exporting Rasterframes](exporting-rasterframes.md)
* [UDF Reference](reference.md)
* [Release Notes](release-notes.md)
@@@

