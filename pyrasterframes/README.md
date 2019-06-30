# PyRasterFrames

## Using PyRasterFrames

See [here](src/main/python/README.md) for user facing details.

## Deployment

The pip installation focuses on local mode operation of spark. To deploy spark applications you will need to consider a few configurations.

### `SparkSession` Setup

#### Python shell


To initialize RasterFrames in a generic Python shell, use the provided convenience function as follows:

```python
from pyrasterframes.utils import create_rf_spark_session
from pyrasterframes.rasterfunctions import *
spark = create_rf_spark_session()
```

If you require further customization you can follow the general pattern below:

```python
from pyspark.sql import SparkSession
from pyrasterframes.utils import find_pyrasterframes_assembly
from pyrasterframes.rasterfunctions import *
rf_jar = find_pyrasterframes_assembly()
spark = (SparkSession.builder
    .master("local[*]")
    .appName("RasterFrames")
    .config("spark.jars", rf_jar)
    .config("spark.some.config.option", "some-value")
    .withKryoSerialization()
    .getOrCreate())
    .withRasterFrames()
```

#### Pyspark shell or app

To quickly get the command to run a `pyspark` shell with PyRasterFrames enabled, run

```bash
sbt pySparkCmd

# > PYTHONSTARTUP=/var/somewhere/pyrf_init.py pyspark --jars <src-root>/pyrasterframes/target/scala-2.11/pyrasterframes-assembly-${VERSION}.jar --py-files <src-root>/pyrasterframes/target/scala-2.11/pyrasterframes-python-${VERSION}.zip
```

The runtime dependencies will be created and the command to run printed to the console.

To manually initialize PyRasterFrames in a `pyspark` shell, prepare to call pyspark with the appropriate `--master` and other `--conf` arguments for your cluster manager and environment. To these you will add the PyRasterFrames assembly JAR and the python source zip. See below for how to build or download those artifacts.

```bash
   pyspark \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.locationtech.rasterframes.util.RFKryoRegistrator \
    --conf spark.kryoserializer.buffer.max=500m \
    --jars pyrasterframes/target/scala-2.11/pyrasterframes-assembly-${VERSION}.jar \
    --py-files pyrasterframes/target/scala-2.11/pyrasterframes-python-${VERSION}.zip
   
```

Then in the pyspark shell or app, import the module and call `withRasterFrames` on the SparkSession.

```python
from pyrasterframes.utils import create_rf_spark_session
spark = create_rf_spark_session()
df = spark.read.raster('https://landsat-pds.s3.amazonaws.com/c1/L8/158/072/LC08_L1TP_158072_20180515_20180604_01_T1/LC08_L1TP_158072_20180515_20180604_01_T1_B5.TIF')
```

## Development

RasterFrames is primarily implemented in Scala, and as such uses the Scala build tool [`sbt`](https://www.scala-sbt.org/).
All `sbt` commands referenced below must be run from the root source directory, i.e. the parent of the `pyrasterframes`
directory, including Python-related build steps.

As a tip, know that `sbt` is much faster if run in "interactive" mode, where you launch `sbt` with no arguments,
and subsequent commands are invoked via an interactive shell. But for context clarity, we'll prefix each command
example below with `sbt`.


## Running Tests

The PyRasterFrames unit tests can found in `<src-root>/pyrasterframes/python/tests`. To run them:

```bash
sbt pyrasterframes/test # alias 'pyTest'
```

*See also the below discussion of running `setup.py` for more options to run unit tests.*

## Running Python Markdown Sources

The markdown documentation in `<src-root>/pyrasterframes/src/main/python/docs` contains code blocks that are evaluated by the build to show results alongside examples. The processed markdown source can be found in `<src-root>/pyrasterframes/target/python/docs`.  

```bash
sbt pyrasterframes/doc # alias 'pyDoc'
```

To build the full complement of documentation and generate the HTML website, run:

```bash
sbt makeSite
``` 

Results will be found in `<src-root>/docs/target/site`.

## Creating and Using a Build

Assuming that `$SCALA_VER` is the major verison of Scala in use (e.g. 2.11) , and `$VER` is the version of RasterFrames, 
the primary build artifacts are:

* JVM library: `<src-root>/pyrasterframes/target/scala-$SCALA_VER/pyrasterframes_$SCALA_VER-$VER.jar`
* Python .whl package with assembly: `<src-root>/pyrasterframes/target/python/dist/pyrasterframes-$VER-py2.py3-none-any.whl`
* Python package as Maven .zip artifact: `<src-root>/pyrasterframes/target/scala-$SCALA_VER/pyrasterframes-python-$VER.zip`

You build them with:

```bash
sbt pyrasterframes/package # alias 'pyBuild'
```

Release versions of these artifacts are published to https://central.sonatype.org/ under the Maven/Ivy groupId:artifactId:version (GAV) coordinates
`org.locationtech.rasterframes:pyrasterframes_$SCALA_VER:$VER`.

Latest version can be found [here](https://search.maven.org/search?q=g:org.locationtech.rasterframes). 
The Python package is published under the `python` classifier, `zip` extension.

### Building and publishing for pypi

To create a source and binary distribution appropriate for pypi:

```bash
sbt pyrasterframes/package # alias 'pyBuild'
```

Observe the output messages such as:

    [info] Python .whl file written to '/Users/monty/rasterframes/pyrasterframes/target/python/dist/pyrasterframes-0.8.0.dev0-py2.py3-none-any.whl'
    
This wheel is suitable for publishing to a package index. See [https://packaging.python.org/tutorials/packaging-projects/#uploading-the-distribution-archives](packaging.python.org).

## Build Internals

### Running `setup.py`

Before a build is initiated, the Python sources are copied to `pyrasterframes/target/python`. This ensures the 
version controlled source directories are not polluted by `setuptools` residuals, better ensuring repeatable builds. To
simplify the process of working with `setup.py` in this context, a `pySetup` sbt interactive command is available. To
illustrate its usage, suppose we want to run a subset of the Python unit test. Instead of running `sbt pyTests` you can:

```bash
sbt 'pySetup test --addopts "-k test_tile_creation"'
```

Or to build a specific document:

```bash
sbt 'pySetup pweave -f docs/raster-io.pymd'
```

*Note: You may need to run `sbt pyrasterframes/package` at least once for certain `pySetup` commands to work.*
