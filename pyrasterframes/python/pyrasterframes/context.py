"""
This module contains access to the jvm SparkContext with RasterFrame support.
"""

from pyspark import SparkContext

def _checked_context():
    """ Get the active SparkContext and throw an error if it is not enabled for RasterFrames."""
    sc = SparkContext._active_spark_context
    if not hasattr(sc, '_rf_context'):
        raise AttributeError(
            "RasterFrames have not been enabled for the active session. Call 'SparkSession.withRasterFrames()'.")
    return sc._rf_context._jrfctx