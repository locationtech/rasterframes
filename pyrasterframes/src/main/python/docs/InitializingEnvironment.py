#' # Initializing PySpark

#' There are a number of ways to use PyRasterFrames:
#'
#' 1. Standalone Script
#' 2. `pyspark` shell
#' 3. Notebook Environment
#'

#' ## Initializing a Python Script

#+ echo=False
from docs import *

#' Most of RasterFrames is implemented in Scala, and compiled to a Java Virtual Machine (JVM)
#' library file called a '.jar' file. The RasterFrames build provides a special `.jar` file,
#' called an "assembly", containing all of RasterFrames as well as the libraries that RasterFrames
#' rquires outside of Spark
#' Let's assume we can locate the PyRasterFrames "assembly" `.jar` file with a function named
#' `find_pyrasterframes_assembly`.

#' The first step is to set up a `SparkSession`:

from pyspark.sql import SparkSession
from pyrasterframes import *
jar = find_pyrasterframes_assembly()
spark = (SparkSession.builder
         .master("local[*]")
         .appName("RasterFrames")
         .config('spark.driver.extraClassPath', jar)
         .config('spark.executor.extraClassPath', jar)
         .withKryoSerialization()
         .getOrCreate())
spark.withRasterFrames()

#' Now we have a standard Spark session with RasterFrames enabled in it.
#' To import RasterFrames functions into the environment, use:
from pyrasterframes.rasterfunctions import *

#' Functions starting with `rf_` (raster-oriented) or `st_` (vector geometry-oriented) are
#' become available for use with DataFrames.

list(filter(lambda x: x.startswith("rf_") or x.startswith("st_"), dir()))

#+ echo=False
spark.stop()
