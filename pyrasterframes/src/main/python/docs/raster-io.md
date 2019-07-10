# Raster Data I/O

The standard mechanism by which any data is brought in and out of a Spark Dataframe is the [Spark SQL DataSource][DS]. RasterFrames are compatible with existing generalized DataSources, such as Parquet, but provide specialized ones oriented around geospatial raster data.

Three types of DataSources will be introduced: 

* Catalog Readers
    - `aws-pds-l8-catalog`: experimental
    - `aws-pds-modis-catalog`: experimental
    - `geotrellis-catalog`: for enumerating [GeoTrellis layers][GTLayer]
* Raster Readers
    - `raster`: the standard reader for most operations
    - `geotiff`: a simplified reader for reading single GeoTIFF files
    - `geotrellis` (for reading [GeoTrellis layers][GTLayer])
* Raster Writers 
    - `geotrellis`: for creating [GeoTrellis layers][GTLayer]
    - `geotiff`: beta
    - `parquet`: for writing raster data in an analytics functional form  

There's also some support for vector data (for masking and data labeling):

* Vector Readers
    - `geojson`: read GeoJson files with `geometry` column aside attribute values


@@@ index
* [Raster Catalogs](raster-catalogs.md)
* [Raster Readers](raster-read.md)
* [Raster Writers](raster-write.md)
@@@


[DS]: https://spark.apache.org/docs/latest/sql-data-sources.html
[GTLayer]: https://geotrellis.readthedocs.io/en/latest/guide/tile-backends.html