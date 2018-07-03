#examples_setup
from pathlib import Path
from pyspark.sql import SparkSession

__all__ = ["resource_dir", "example_session"]

jarpath = list(Path('../target').resolve().glob('**/pyrasterframes*.jar'))
if len(jarpath) > 0:
    pyJar = jarpath[0].as_uri()
    def example_session():
        return (SparkSession.builder
        .master("local[*]")
        .appName("RasterFrames")
        .config('spark.driver.extraClassPath', pyJar)
        .config('spark.executor.extraClassPath', pyJar)
        .config("spark.ui.enabled", "false")
        .getOrCreate())


# hard-coded relative path for resources
resource_dir = Path('./static').resolve()
#examples_setup
