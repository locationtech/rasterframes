# PyRasterFrames

PyRasterFrames is a library for distributed processing of geospatial raster data with Spark.


## Prerequisites

1. [`pip`](https://pip.pypa.io/en/stable/installing/)
2. ['pyspark`](https://pypi.org/project/pyspark/) > 2.3.2 

## Quickstart

The quickest way to get started is to `pip` install the pyrasterframes package.

```bash
pip install pyrasterframes
```

You can then access a [`pyspark SparkSession`]() using the [`local[*]` master](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) as follows.

```python
import pyrasterframes
spark = pyrasterframes.get_spark_session()
```

Then you can read a raster and do some simple processing on it.

```python
from pyrasterframes.rasterfunctions import *
from pyspark.sql.functions import lit
# Read a Landsat 8 L1TP PDS scene
df = spark.read.rastersource('https://landsat-pds.s3.amazonaws.com/c1/L8/038/037/LC08_L1TP_038037_20190322_20190403_01_T1/LC08_L1TP_038037_20190322_20190403_01_T1_B4.TIF')
# Add 3 to every cell, show some rows of the dataframe
df.select(rf_local_add(df.tile, lit(3))).show(6, False)
```

## Development

RasterFrames is primarily implemented in Scala, and as such uses the Scala build tool [`sbt`](https://www.scala-sbt.org/).
All `sbt` commands referenced below must be run from the root source directory, i.e. the parent of the `pyrasterframes`
directory, including Python-related build steps.

As a tip, know that `sbt` is much faster if run in "interactive" mode, where you launch `sbt` with no arguments,
and subsequent commands are invoked via an interactive shell. But for context clarity, we'll prefix each command
example below with `sbt`.


## Running Tests and Examples

The PyRasterFrames unit tests can found in `<src-root>/pyrasterframes/python/tests`. To run them:

```bash
sbt pyTests
```

*See also the below discussion of running `setup.py` for more options to run unit tests.* 

Similarly, to run the examples in `<src-root>pyrasterframes/python/examples`:

```bash
sbt pyExamples
```

## Creating and Using a Build

Assuming that `$SCALA_VER` is the major verison of Scala in use (e.g. 2.11) , and `$VER` is the version of RasterFrames, 
the primary build artifacts are:

* JVM library: `pyrasterframes/target/scala-$SCALA_VER/pyrasterframes_$SCALA_VER-$VER.jar`
* Python package: `pyrasterframes/target/scala-$SCALA_VER/pyrasterframes-python-$VER.zip`

You build them with:

```bash
sbt pyrasterframes/package
```

Release versions of these artifacts are published to https://central.sonatype.org/ under the Maven/Ivy groupId:artifactId:version (GAV) coordinates
`org.locationtech.rasterframes:pyrasterframes_$SCALA_VER:$VER`.

Latest version can be found [here](https://search.maven.org/search?q=g:org.locationtech.rasterframes). 
The Python package is published under the `python` classifier, `zip` extension.

## Build Internals

### Running `setup.py`

Before a build is initiated, the Python sources are copied to `pyrasterframes/target/python`. This ensures the 
version controlled source directories are not polluted by `setuptools` residuals, better ensuring repeatable builds. To
simplify the process of working with `setup.py` in this context, a `pySetup` sbt interactive command is available. To
illustrate its usage, suppose we want to run a subset of the Python unit test. Instead of running `sbt pyTests` you can:

```bash
sbt 'pySetup test --addopts "-k test_tile_creation"'
```

Or to run a specific example:

```bash
sbt 'pySetup examples -e NDVI'
```

*Note: You may need to run `sbt pyrasterframes/assembly` at least once for certain `pySetup` commands to work.*

## `SparkSession` Setup

### Python shell

To initialize PyRasterFrames in a generic Python shell:

```python
from pyspark.sql import SparkSession
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
spark = SparkSession.builder \
     .master("local[*]") \
     .appName("Using RasterFrames") \
     .config("spark.some.config.option", "some-value") \
     .withKryoSerialization() \
     .getOrCreate() \
     .withRasterFrames()
```

### Pyspark shell

To initialize PyRasterFrames in a `pyspark` shell, prepare to call pyspark with the appropriate `--master` and other `--conf` arguments for your cluster manager and environment. To these you will add the PyRasterFrames assembly JAR and the pyton source zip.

```bash
   pyspark \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.locationtech.rasterframes.util.RFKryoRegistrator \
    --conf spark.kryoserializer.buffer.max=500m \
    --jars pyrasterframes/target/scala-2.11/pyrasterframes-assembly-${VERSION}.jar \
    --py-files pyrasterframes/target/scala-2.11/pyrasterframes-python-${VERSION}.zip
   
```

Then in the pyspark shell import the module and call `withRasterFrames`.

```python
import pyrasterframes
spark = spark.withRasterFrames()
df = spark.read.rastersource('https://landsat-pds.s3.amazonaws.com/c1/L8/158/072/LC08_L1TP_158072_20180515_20180604_01_T1/LC08_L1TP_158072_20180515_20180604_01_T1_B5.TIF')
```
