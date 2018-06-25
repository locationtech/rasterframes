==============
PyRasterFrames
==============

PyRasterFrames provides a Python API for RasterFrames!

------------
SBT Commands
------------

Build the shaded JAR:

    ``$ sbt pyrasterframes/spPublishLocal``

Run tests:

    ``$ sbt pyrasterframes/pyTest``

Run examples:

    ``$ sbt pyrasterframes/pyExamples``


---------------
Python Commands
---------------

Install the python package (for development / local use):

    ``$ pip install -e python``


To run tests and examples, ``$ cd python``, then:

Run tests (if no Scala code has changed):

    ``$ python setup.py test``

    To run verbosely:
    ``$ python setup.py test --addopts -s``

    To run a specific test:
    ``$ python setup.py test --addopts "-k my_test_name"``


Run examples:

    ``$ python setup.py examples [--e EXAMPLENAME,EXAMPLENAME]``


-----------
Spark Usage
-----------

Get a Spark REPL (after running ``$ sbt pyrasterframes/spPublishLocal``):

    ``$  pyspark --packages io.astraea:pyrasterframes:$VERSION --master local[2]``

You can then try some of the commands in ``python/tests/PyRasterFramesTests.py``.

Submit a script (from the ``python`` directory):

    ``$ spark-submit --packages io.astraea:pyrasterframes:$VERSION --master local[2] \
        examples/CreatingRasterFrames.py``

To initialize PyRasterFrames:

    >>> from pyrasterframes import *
    >>> spark = SparkSession.builder \
    ...     .master("local[*]") \
    ...     .appName("Using RasterFrames") \
    ...     .config("spark.some.config.option", "some-value") \
    ...     .getOrCreate() \
    ...     .withRasterFrames()

