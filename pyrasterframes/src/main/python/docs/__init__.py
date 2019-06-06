import os
import glob
from pyspark.sql import SparkSession

__all__ = ["create_spark_session", "find_pyrasterframes_assembly"]

def find_pyrasterframes_assembly():
    here = os.path.dirname(os.path.realpath(__file__))
    scala_target = os.path.realpath(os.path.join(here, '..', '..', 'scala-2.11'))
    jarpath = glob.glob(os.path.join(scala_target, 'pyrasterframes-assembly*.jar'))
    if not len(jarpath) == 1:
        raise RuntimeError("""
Expected to find exactly one assembly. Found '{}' instead. 
Try running 'sbt pyrasterframes/package' first. """.format(jarpath))
    return jarpath[0]

def create_spark_session():
    pyJar = find_pyrasterframes_assembly()
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("RasterFrames")
             .config('spark.driver.extraClassPath', pyJar)
             .config('spark.executor.extraClassPath', pyJar)
             .config("spark.ui.enabled", "false")
             .withKryoSerialization()
             .getOrCreate())
    spark.withRasterFrames()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

