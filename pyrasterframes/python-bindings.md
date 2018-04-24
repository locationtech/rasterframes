# RasterFrames Python Bindings

## Goals

The first goal of this effort is obvious: make the Scala-implemented features of RasterFrames available to Python programmers, via `pyspark` and `spark-submit`. The aim is to do so in way that is both idiomatic Python, yet synchronous with the Scala implementation and mental model.

The second goal is to provide both embedded "python docstring" documentation and extensive Users' Manual examples of the Python-enabled RasterFrames features.

The third goal is package and deploy this capability in a form that is the minimum possible amount of friction for new users to give it a spin. Something like 

    pyspark --packages io.astraea:rasterframes:1.2.3
    
would be fantastic.

The collection of these goals defines the über-goal: inspire and facilitate adoption of RasterFrames for solving real-world problems.

## Implementing Bindings

A proof-of concept implementation has already been started on the `feature/python-api` branch.
See the instructions [here](https://github.com/s22s/raster-frames/tree/feature/python-api/python/README.rst) for how to run it. 

One of the sub-goals of the implementation is to have as little of it as possible! There are two types of bindings: Scala class wrappers, and UDF references. The former requires attention to delivering RasterFrames extension methods on Dataframes in a Pythonic way, [wrapping them](https://github.com/s22s/raster-frames/blob/feature/python-api/python/pyrasterframes/__init__.py) in Python classes, and handling type conversions via the Py4J API, as necessary. The latter simply requires adding a symbolic reference to the UDF namd in [`functions.py`](https://github.com/s22s/raster-frames/blob/feature/python-api/python/pyrasterframes/functions.py).

The primary wrapper classes are [`RFContext`](https://github.com/s22s/raster-frames/blob/47a6f1af4c601ce75baf32b440bc3943d6e92f09/python/pyrasterframes/__init__.py#L12-L22) and [`RasterFrame`](https://github.com/s22s/raster-frames/blob/47a6f1af4c601ce75baf32b440bc3943d6e92f09/python/pyrasterframes/__init__.py#L25-L52). `RFContext` is made available as  `spark_session.rf` when `SparkSession.withRasterFrames` is called. It is through this instance that `RasterFrame` instances are created.

The `RasterFrame` wrapper is primarily there to expose extension methods on Scala side RasterFrames, and maintain the `RasterFrame` invariants over `TileLayerMetadata`.

## Documenting Bindings

RasterFames has a baseline [Users' Manual](http://rasterframes.io/index.html), built from Markdown files using a Scala tool called [Paradox](https://developer.lightbend.com/docs/paradox/latest/index.html). This tool has a special feature called ["Groups"](https://developer.lightbend.com/docs/paradox/latest/features/groups.html
) for showing code snippets in multiple Programming languages. A possible approach to fulfilling this goal is to take the existing Scala examples and reimplement them in Python. Most of these examples live in full source form in an [examples directory](https://github.com/s22s/raster-frames/tree/develop/src/test/scala/examples). Once the example is working, it is manually reformatted into Markdown (in the case of Scala, we have a tool that will evaluate the code and inject the results automatically). 

A stretch goal is to also have the examples written in SQL.

From a pure Python perspective, we'd like to have whatever is customary for a mature Python library.

## Deploying Bindings

We eventually want RasterFrames deployed via https://spark-packages.org/, in a form supporting both languages. We will be looking at https://github.com/databricks/sbt-spark-package to assist with the process.

## Tasks

* [x] Implement `asRF` on `Dataframe`
* [x] Expose RasterFrame extension methods
* [x] Add mirror for TileUDT (and update `pyUDT` field in Scala)
* [ ] Properly (pythondoc) document wrapper classes
* [ ] Enable language groups in Paradox
* [x] Implement Scala examples in Python, finding missing API bindings
* [ ] ~~Publish via spark-packages.org~~
* [x] Figure out how to unit test python code


## References

* https://www.py4j.org/
* http://aseigneurin.github.io/2016/09/01/spark-calling-scala-code-from-pyspark.html
* https://github.com/harsha2010/magellan/tree/master/python
* https://github.com/locationtech/geomesa/tree/master/geomesa-spark/geomesa_pyspark/src/main/python/geomesa_pyspark
* https://github.com/locationtech-labs/geopyspark
* https://stackoverflow.com/a/36024707/296509
