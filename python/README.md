# PyRasterFrames

PyRasterFrames enables access and processing of geospatial raster data in PySpark DataFrames.

## Getting started

The quickest way to get started is to [`pip`](https://pip.pypa.io/en/stable/installing/) install the pyrasterframes package.

```bash
pip install pyrasterframes
```

You can then access a [`pyspark SparkSession`](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession) using the [`local[*]` master](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) in your python interpreter as follows.

```python
from pyrasterframes.utils import create_rf_spark_session
spark = create_rf_spark_session()
```

Then you can read a raster and do some work with it.

```python
from pyrasterframes.rasterfunctions import *
from pyspark.sql.functions import lit
# Read a MODIS surface reflectance granule
df = spark.read.raster('https://modis-pds.s3.amazonaws.com/MCD43A4.006/11/08/2019059/MCD43A4.A2019059.h11v08.006.2019072203257_B02.TIF')
# Add 3 element-wise, show some rows of the dataframe
df.select(rf_local_add(df.tile, lit(3))).show(5, False)
```

## Support

Reach out to us on [gitter][gitter]!

Issue tracking is through [github](https://github.com/locationtech/rasterframes/issues).

## Contributing

Community contributions are always welcome. To get started, please review our [contribution guidelines](https://github.com/locationtech/rasterframes/blob/develop/CONTRIBUTING.md), [code of conduct](https://github.com/locationtech/rasterframes/blob/develop/CODE_OF_CONDUCT.md), and [developer's guide](../../../README.md).  Reach out to us on [gitter][gitter] so the community can help you get started!

## Development environment setup

For best results, we suggest using `conda` and the `conda-forge` channel to install the compiled dependencies before installing the packages in `setup.py`. Assuming you're in the same directory as this file:

    conda create -n rasterframes python==3.7
    conda install --file ./requirements-condaforge.txt

Then you can install the source dependencies:

    pip install -e .

[gitter]: https://gitter.im/locationtech/rasterframes
