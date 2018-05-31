PyRasterFrames
--------------

PyRasterFrames provides a Python API for RasterFrames!

Getting started

Build the shaded JAR.

    $ sbt assembly

Install the python package (for development / local use)

    $ pip install -e python

Get a Spark REPL

    $ pyspark --jars target/scala-2.11/pyrasterframes-assembly-$VERSION.jar --master local[2]

You can then try some of the commands in `tests/PyRasterFramesTests.py`.

Submit a script

    $ spark-submit --jars target/scala-2.11/pyrasterframes-assembly-$VERSION.jar --master local[2] \
        python/examples/CreatingRasterFrames.py

Run tests

    $ sbt pyTest

    OR

    $ python setup.py test
    $ # To run verbosely:
    $ python setup.py test --addopts -s
    $ # To run a specific test:
    $ python setup.py test --addopts "-k my_test_name"

Run examples

    $ sbt pyExamples

    OR

    $ python setup.py examples [--e EXAMPLENAME,EXAMPLENAME]


To initialize PyRasterFrames:

    >>> from pyrasterframes import *
    >>> spark = SparkSession.builder \
    ...     .master("local[*]") \
    ...     .appName("Using RasterFrames") \
    ...     .config("spark.some.config.option", "some-value") \
    ...     .getOrCreate() \
    ...     .withRasterFrames()

