<img src="docs/src/main/paradox/_template/images/RasterFramesLogo.png" width="300px"/><sup style="vertical-align: top;">&trade;</sup>

 [![Join the chat at https://gitter.im/s22s/raster-frames](https://badges.gitter.im/s22s/raster-frames.svg)](https://gitter.im/s22s/raster-frames?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

_RasterFrames™_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of [GeoTrellis](https://geotrellis.io/).

<img src="docs/src/main/paradox/RasterFramePipeline.svg" width="600px"/>

Please see the [Getting Started](http://rasterframes.io/getting-started.html) section of the Users' Manual to start using RasterFrames.

## Documentation

* [Users' Manual](http://rasterframes.io/)
* [API Documentation](http://rasterframes.io/latest/api/index.html) 
* [List of available UDFs](http://rasterframes.io/latest/api/index.html#org.locationtech.rasterframes.RasterFunctions)
* [RasterFrames Jupyter Notebook Docker Image](https://hub.docker.com/r/s22s/rasterframes-notebooks/) 

## Build instruction

First, you will need [sbt](https://www.scala-sbt.org/).

Download the source from GitHub.

```bash
git clone https://github.com/locationtech/rasterframes.git
cd rasterframes
```

To publish to hyou Ivy local repository:

```bash
sbt publishLocal
```

You can run tests with

```bash
sbt test
```

and integration tests

```bash
sbt it:test
```

The documentation may be built with

```bash
sbt makeSite
```

The `pyrasterframes` build instructions are located [pyrasterframes/python/README.rst](pyrasterframes/python/README.rst)

## Copyright and License

RasterFrames is released under the Apache 2.0 License, copyright Astraea, Inc. 2017-2018.


