# PyRasterFrames

PyRasterFrames is the Python API for Spark RasterFrames. 

Building RasterFrames requires [`sbt`](https://www.scala-sbt.org/), and all `sbt` commands referenced below must be 
run from the root source directory, i.e. the parent of the `pyrasterframes` directory. Know that `sbt` is much 
faster if run in "interactive" mode, where you launch `sbt` with no arguments, and subsequent commands are invoked
via an interactive shell. But for context clarity, we'll prefix each command example below with `sbt`. 

## Quickstart

The quickest way to run a `pyspark` shell with the latest RasterFrames enabled is to run:

```bash
sbt pySparkCmd
```

This will: 

1. Compile all the Scala/JVM code.
1. Merge all JVM code and dependencies into a single "assembly" JAR file.
1. Create the PyRasterFrames `.whl` package
1. Construct a temporary initialization script
1. Emit a `bash` command with requiesite arguments to load RasterFrames and import PyRasterFrames 

You then copy/paste the emitted command into your `bash` shell to start up a spark shell. It assumes you have
`pyspark` >= 2.3.2 installed in your environment.

## Running Tests and Examples

The PyRasterFrames unit tests can found in `<src-root>/pyrasterframes/python/tests`. To run them:

```bash
sbt pyTests
```

Similarly, to run the examples in `<src-root>pyrasterframes/python/examples`:

```bash
sbt pyExamples
```

## Creating and Using a Build

Assuming that `$SCALA_VER` is the major verison of Scala in use, and `$VER` is the version of RasterFrames, 
the primary build artifacts are:

* JVM library: `pyrasterframes/target/scala-$SCALA_VER/pyrasterframes_$SCALA_VER-$VER.jar`
* Python package: `pyrasterframes/target/scala-$SCALA_VER/pyrasterframes-python-$VER.zip`

You build them with"

```bash
sbt pyrasterframes/package
```

Release versions of these artifacts are published to https://central.sonatype.org/ under the Maven/Ivy "GAV" coordinates
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

### `SparkSession` Setup

To initialize PyRasterFrames in a generic Python shell:

```python
from pyspark.sql import SparkSession
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
spark = SparkSession.builder \
     .master("local[*]") \
     .appName("Using RasterFrames") \
     .config("spark.some.config.option", "some-value") \
     .getOrCreate() \
     .withRasterFrames()
```

