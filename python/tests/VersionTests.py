import os

from pyspark.version import __version__ as pyspark_version


def test_spark_version(spark):
    assert spark.version == os.environ["SPARK_VERSION"]


def test_pyspark_version():
    assert pyspark_version == os.environ["SPARK_VERSION"]
