"""
This module contains access to the jvm SparkContext with RasterFrame support.
"""

from pyspark import SparkContext

__all__ = ['RFContext']


class RFContext(object):
    """
    Entrypoint to RasterFrames services
    """
    def __init__(self, spark_session):
        self._spark_session = spark_session
        self._gateway = spark_session.sparkContext._gateway
        self._jvm = self._gateway.jvm
        jsess = self._spark_session._jsparkSession
        self._jrfctx = self._jvm.astraea.spark.rasterframes.py.PyRFContext(jsess)

    def list_to_seq(self, py_list):
        conv = self.lookup('listToSeq')
        return conv(py_list)

    def lookup(self, function_name):
        return getattr(self._jrfctx, function_name)

    @staticmethod
    def active():
        """
        Get the active Python RFContext and throw an error if it is not enabled for RasterFrames.
        """
        sc = SparkContext._active_spark_context
        if not hasattr(sc, '_rf_context'):
            raise AttributeError(
                "RasterFrames have not been enabled for the active session. Call 'SparkSession.withRasterFrames()'.")
        return sc._rf_context

    @staticmethod
    def call(name, *args):
        f = RFContext.active().lookup(name)
        return f(*args)

    @staticmethod
    def _jvm_mirror():
        """
        Get the active Scala PyRFContext and throw an error if it is not enabled for RasterFrames.
        """
        return RFContext.active()._jrfctx

