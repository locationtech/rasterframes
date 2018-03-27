# Getting Started

## Quick Start

### macOS

1. If not already, install [Homebrew](https://brew.sh/)
2. Run `brew install sbt`
3. Run `sbt new s22s/raster-frames.g8`

### Linux

1. Install [sbt](http://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html)
2. Run `sbt new s22s/raster-frames.g8`

### Windows

1. Install [sbt](http://www.scala-sbt.org/release/docs/Installing-sbt-on-Windows.html)
2. Run `sbt new s22s/raster-frames.g8`

## General Setup

*RasterFrames* is published via [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Craster-frames).

To use RasterFrames, add the following library dependencies:

@@dependency[sbt,Maven,Gradle] {
  group="io.astraea"
  artifact="raster-frames_2.11"
  version="x.y.z"
}

@@dependency[sbt,Maven,Gradle] {
  group="io.astraea"
  artifact="raster-frames-datasource_2.11"
  version="x.y.z"
}

It assumes that SparkSQL 2.2.x is available in the runtime classpath. Here's how to add it explicitly:

@@dependency[sbt,Maven,Gradle] {
  group="org.apache.spark"
  artifact="spark-sql"
  version="2.2.0"
}

@@@ note
Most of the following examples are shown using the Spark DataFrames API. However, many could also be rewritten to use the Spark SQL API instead. We hope to add more examples in that form in the future.
@@@
