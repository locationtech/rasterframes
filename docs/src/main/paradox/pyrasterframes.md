# RasterFrames&nbsp;in&nbsp;Python

RasterFrames includes Python bindings for using the library from within [PySpark](https://spark.apache.org/docs/latest/api/python/pyspark.html).
While creating Python documentation on parity with Scala is forthcoming, these resources should help 
in the meantime:

* [PyRasterFrames README](https://github.com/locationtech/rasterframes/blob/develop/pyrasterframes/python/README.rst)
* [PyRasterFrames Examples](https://github.com/locationtech/rasterframes/tree/develop/pyrasterframes/python/examples)
* [RasterFrames Jupyter Notebook](https://github.com/locationtech/rasterframes/blob/develop/rf-notebook/README.md)
* @ref:[PyRasterFrames Functions](reference.md)

Most features available in the Scala API are exposed in the Python API, refer to the @ref:[function reference](reference.md). Defining a [udf](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf) using a `Tile` column through the Python API is not yet supported. 

If there's a specific feature that appears to be missing in the Python version [please submit an issue](https://github.com/locationtech/rasterframes/issues) so that we might address it for you.
