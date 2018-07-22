# Getting&nbsp;Started

## Quick Start

RasterFrames is available in in a Jupyter Notebook Docker container for quick experimentation.

1. Install [Docker](https://www.docker.com/get-docker) for your OS flavor.
2. Run  
   `docker run -it --rm -p 8888:8888 -p 4040-4044:4040-4044 s22s/rasterframes-notebooks`

Additional instructions can be found [here](https://github.com/locationtech/rasterframes/blob/develop/deployment/README.md).

## General Setup

*RasterFrames* is published via [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Crasterframes) (click link to see latest versions).

To use RasterFrames, add the following library dependencies:

@@dependency[sbt,Maven,Gradle] {
  group="org.locationtech.rasterframes"
  artifact="rasterframes_2.11"
  version="$version$"
}

@@dependency[sbt,Maven,Gradle] {
  group="org.locationtech.rasterframes"
  artifact="rasterframes-datasource_2.11"
  version="$version$"
}

Optional:

@@dependency[sbt,Maven,Gradle] {
  group="org.locationtech.rasterframes"
  artifact="rasterframes-experimental_2.11"
  version="$version$"
}

It assumes that SparkSQL 2.2.x is available in the runtime classpath. Here's how to add it explicitly:

@@dependency[sbt,Maven,Gradle] {
  group="org.apache.spark"
  artifact="spark-sql"
  version="2.2.1"
}

@@@ note
Most of the following examples are shown using the Spark DataFrames API. However, many could also be rewritten to use the Spark SQL API instead. We hope to add more examples in that form in the future.
@@@
