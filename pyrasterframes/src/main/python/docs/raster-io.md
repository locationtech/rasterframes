# Raster Data I/O

The standard mechanism by which any data is brought in and out of a Spark Dataframe is the [Spark SQL DataSource][DS]. RasterFrames provides specialized DataSources for geospatial raster data and maintains compatibility with existing general purpose DataSources, such as Parquet.

* @ref:[Catalog Readers](raster-catalogs.md)
    - `aws-pds-l8-catalog`: built-in catalog over [Landsat on AWS][Landsat]
    - `aws-pds-modis-catalog`: built-in catalog over [MODIS on AWS][MODIS]
    - `geotrellis-catalog`: for enumerating [GeoTrellis layers][GTLayer]
* @ref:[Raster Readers](raster-read.md)
    - `raster`: the standard reader for most raster data, including single raster files or catalogs
    - `geotiff`: a simplified reader for reading a single GeoTIFF file
    - `geotrellis`: for reading a [GeoTrellis layer][GTLayer]
* @ref:[Raster Writers](raster-write.md)
    - You can write @ref:[Tile](raster-write.md#tile-samples) and @ref:[DataFrame](raster-write.md#dataframe-samples) samples
    - @ref:[`geotiff`](raster-write.md#geotiffs): beta writer to GeoTiff file format
    - @ref:[`geotrellis`](raster-write.md#geotrellis-layers): creating a [GeoTrellis layer][GTLayer]
    - @ref:[`parquet`](raster-write.md#parquet): general purpose writer for [Parquet][Parquet]

There is also support for @ref:[vector data](vector-data.md) for masking and data labeling.

@@@ index
* @ref:[Raster Catalogs](raster-catalogs.md)
* @ref:[Raster Readers](raster-read.md)
* @ref:[Raster Writers](raster-write.md)
@@@


[DS]: https://spark.apache.org/docs/latest/sql-data-sources.html
[GTLayer]: https://geotrellis.readthedocs.io/en/latest/guide/tile-backends.html
[Parquet]: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
[MODIS]: https://docs.opendata.aws/modis-pds/readme.html
[Landsat]: https://docs.opendata.aws/landsat-pds/readme.html
