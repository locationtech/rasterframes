# examples_setup
import os
import glob
from pyspark.sql import SparkSession

__all__ = ["resource_dir", "example_session"]

HERE = os.path.dirname(os.path.realpath(__file__))

scala_target = os.path.realpath(os.path.join(HERE, '..', '..', 'scala-2.11'))
resource_dir = os.path.realpath(os.path.join(scala_target, 'test-classes'))
jarpath = glob.glob(os.path.join(scala_target, 'pyrasterframes-assembly*.jar'))

if not len(jarpath) == 1:
    raise RuntimeError("""
Expected to find exactly one assembly. Found '{}' instead. 
Try running 'sbt pyrasterframes/clean' first. """.format(jarpath))

pyJar = jarpath[0]


def example_session():
    return (SparkSession.builder
            .master("local[*]")
            .appName("RasterFrames")
            .config('spark.driver.extraClassPath', pyJar)
            .config('spark.executor.extraClassPath', pyJar)
            .config("spark.ui.enabled", "false")
            .withKryoSerialization()
            .getOrCreate())

