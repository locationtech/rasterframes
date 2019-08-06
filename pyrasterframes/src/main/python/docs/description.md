# Overview

RasterFrames® provides a DataFrame-centric view over arbitrary Earth-observation (EO) data, enabling spatiotemporal queries, map algebra raster operations, and compatibility with the ecosystem of [Apache Spark](https://spark.apache.org/docs/latest/) [ML](https://spark.apache.org/docs/latest/ml-guide.html) algorithms. It provides APIs in @ref:[Python, SQL, and Scala](languages.md), and can scale from a laptop to a large distributed cluster, enabling _global_ analysis with satellite imagery in a wholly new, flexible and convenient way.

## Context

We have a millennia-long history of organizing information in tabular form. Typically, rows represent independent events or observations, and columns represent attributes and measurements from the observations. The forms have evolved, from hand-written agricultural records and transaction ledgers, to the advent of spreadsheets on the personal computer, and on to the creation of the _DataFrame_ data structure as found in [R Data Frames][R] and [Python Pandas][Pandas]. The table-oriented data structure remains a common and critical component of organizing data across industries, and is the mental model employed by many data scientists across diverse forms of modeling and analysis.

The evolution of the DataFrame form has continued with [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html), which brings DataFrames to the big data distributed compute space. Through several novel innovations, Spark SQL enables data scientists to work with DataFrames too large for the memory of a single computer. As suggested by the name, these DataFrames are manipulatable via standard SQL, as well as the more general-purpose programming languages Python, R, Java, and Scala.

RasterFrames, an incubating Eclipse Foundation LocationTech project, brings together EO data access, cloud computing, and DataFrame-based data science. The recent explosion of EO data from public and private satellite operators presents both a huge opportunity as well as a challenge to the data analysis community. It is _Big Data_ in the truest sense, and its footprint is rapidly getting bigger. According to a World Bank document on assets for post-disaster situation awareness[^1]:

> Of the 1,738 operational satellites currently orbiting the earth (as of 9/[20]17), 596 are earth observation satellites and 477 of these are non-military assets (ie available to civil society including commercial entities and governments for earth observation, according to the Union of Concerned Scientists). This number is expected to increase significantly over the next ten years. The 200 or so planned remote sensing satellites have a value of over 27 billion USD (Forecast International). This estimate does not include the burgeoning fleets of smallsats as well as micro, nano and even smaller satellites... All this enthusiasm has, not unexpectedly, led to a veritable fire-hose of remotely sensed data which is becoming difficult to navigate even for seasoned experts.

## Benefit

By using DataFrames as the core cognitive and compute data model for processing EO data, RasterFrames is able to deliver sophisticated computational and algorithmic capabilities in a tabular form that is familiar and accessible to the general computing public. Because it is built on Apache Spark, solutions prototyped on a laptop can be easily scaled to run on cluster and cloud compute resources. Apache Spark also provides integration between its DataFrame libraries and machine learning, with which RasterFrames is fully compatible.

## Architecture

RasterFrames builds upon several other LocationTech projects:
[GeoTrellis](https://geotrellis.io/), [GeoMesa](https://www.geomesa.org/),
[JTS](https://github.com/locationtech/jts), and
[SFCurve](https://github.com/locationtech/sfcurve).

![LocationTech Stack](static/rasterframes-locationtech-stack.png)

RasterFrames introduces georectified raster imagery to Spark SQL. It quantizes scenes into chunks called @ref:[_tiles_](concepts.md#tile). Each tile contains a 2-D matrix of @ref:[_cell_](concepts.md#tile) or pixel values along with information on how to numerically interpret those cells. 

As shown in the figure below, a "RasterFrame" is a Spark DataFrame with one or more columns of type @ref:[`tile`](concepts.md#tile). A `tile` column typically represents a single frequency band of sensor data, such as "blue" or "near infrared", but can also be quality assurance information, land classification assignments, or any other raster spatial data. Along with `tile` columns there is typically an @ref:[`extent`](concepts.md#extent) specifying the geographic location of the data, the map projection of that geometry (@ref:[`crs`](concepts.md#coordinate-reference-system--crs-)), and a `timestamp` column representing the acquisition time. These columns can all be used in the `WHERE` clause when filtering.

@@include[RasterFrame Example](static/rasterframe-sample.md)

RasterFrames also includes support for working with vector data, such as [GeoJSON][GeoJSON]. RasterFrames vector data operations let you filter with geospatial relationships like contains or intersects, mask cells, convert vectors to rasters, and more.

Raster data can be read from a @ref:[number of sources](raster-io.md). Through the flexible Spark SQL DataSource API, RasterFrames can be constructed from collections of imagery (including Cloud Optimized GeoTIFFs or [COGS][COGS]), [GeoTrellis Layers][GTLayer], and from catalogs of large datasets like Landsat 8 and MODIS data sets on the @ref:[AWS Public Data Set (PDS)](raster-catalogs.md#using-external-catalogs). 

[R]:https://www.rdocumentation.org/packages/base/versions/3.5.1/topics/data.frame
[Pandas]:https://pandas.pydata.org/
[GeoJSON]:https://en.wikipedia.org/wiki/GeoJSON
[GTLayer]:https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html#layouts-and-tile-layers
[PDS]:https://registry.opendata.aws/modis/
[COGS]:https://www.cogeo.org/

[^1]: [_Demystifying Satellite Assets for Post-Disaster Situation Awareness_](https://docs.google.com/document/d/11bIw5HcEiZy8SKli6ZFQC2chVEiiIJ-f0o6btA4LU48).
World Bank via [OpenDRI.org](https://opendri.org/resource/demystifying-satellite-assets-for-post-disaster-situation-awareness/). Accessed November 28, 2018.
