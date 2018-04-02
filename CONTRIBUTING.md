# Contributing to RasterFrames

Thanks for your interest in this project.

## Project Description

LocationTech RasterFrames brings the power of Spark DataFrames to geospatial
raster data, empowered by the map algebra and tile layer operations of
GeoTrellis. The underlying purpose of RasterFrames is to allow data scientists
and software developers to process and analyze geospatial-temporal raster data
with the same flexibility and ease as any other Spark Catalyst data type. At its
core is a user-defined type (UDT) called TileUDT, which encodes a GeoTrellis
Tile in a form the Spark Catalyst engine can process. Furthermore, we extend the
definition of a DataFrame to encompass some additional invariants, allowing for
geospatial operations within and between RasterFrames to occur, while still
maintaining necessary geo-referencing constructs.

 * https://projects.eclipse.org/projects/locationtech.rasterframes
 
## Eclipse Contributor Agreement

Before your contribution can be accepted by the project team contributors must
electronically sign the Eclipse Contributor Agreement (ECA).

 * http://www.eclipse.org/legal/ECA.php

Commits that are provided by non-committers must have a Signed-off-by field in
the footer indicating that the author is aware of the terms by which the
contribution has been provided to the project. The non-committer must
additionally have an Eclipse Foundation account and must have a signed Eclipse
Contributor Agreement (ECA) on file.

For more information, please see the Eclipse Committer Handbook:
https://www.eclipse.org/projects/handbook/#resources-commit

## Developer Resources

The RasterFrames source code is hosted on GitHub:

 * https://github.com/locationtech/rasterframes
 
Issues should be submitted via GitHub issues:

 * https://github.com/locationtech/rasterframes/issues
 
A user manual may be found here:
 
 * http://rasterframes.io
 
## Building

RasterFrames uses [sbt](https://www.scala-sbt.org/) as its build tool. Standard build
commands are as follows:

* Compile: `sbt compile`
* Install packages locally: `sbt publishLocal`
* Run tests: `sbt test`
* Build documentation: `sbt makeSite`
* Spark shell with RasterFrames initialized: `sbt console`

 
## Contribution Process

RasterFrames uses GitHub pull requests (PRs) for accepting contributions. 
Please fork the repository, create a branch, and submit a PR based off the `master` branch.
During the PR review process comments may be attached. Please look out for comments
and respond as necessary.
 

## Contact

Help, questions and community dialog are supported via Gitter:

 * https://gitter.im/s22s/raster-frames

Commercial support is available by writing to info@astraea.earth
