#
# This software is licensed under the Apache 2 license, quoted below.
#
# Copyright 2019 Astraea, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# [http://www.apache.org/licenses/LICENSE-2.0]
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# SPDX-License-Identifier: Apache-2.0
#

"""
This module contains access to the jvm SparkContext with RasterFrameLayer support.
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession

from typing import Any, List
from py4j.java_gateway import JavaMember
from py4j.java_collections import JavaList, JavaMap
from typing import Tuple

__all__ = ['RFContext']


class RFContext(object):
    """
    Entrypoint to RasterFrames services
    """
    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        self._gateway = spark_session.sparkContext._gateway
        self._jvm = self._gateway.jvm
        jsess = self._spark_session._jsparkSession
        self._jrfctx = self._jvm.org.locationtech.rasterframes.py.PyRFContext(jsess)

    def list_to_seq(self, py_list: List[Any]) -> JavaList:
        conv = self.lookup('_listToSeq')
        return conv(py_list)

    def lookup(self, function_name: str) -> JavaMember:
        return getattr(self._jrfctx, function_name)

    def build_info(self) -> JavaMap:
        return self._jrfctx.buildInfo()

    def companion_of(self, classname: str):
        if not classname.endswith("$"):
            classname = classname + "$"
        companion_module = getattr(self._jvm, classname)
        singleton = getattr(companion_module, "MODULE$")
        return singleton

    # NB: Tightly coupled to `org.locationtech.rasterframes.py.PyRFContext._resolveRasterRef`
    def _resolve_raster_ref(self, ref_struct):
        f = self.lookup("_resolveRasterRef")
        return f(
            ref_struct.source.raster_source_kryo,
            ref_struct.bandIndex,
            ref_struct.subextent.xmin,
            ref_struct.subextent.ymin,
            ref_struct.subextent.xmax,
            ref_struct.subextent.ymax,
        )

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
    def jvm():
        """
        Get the active Scala PyRFContext and throw an error if it is not enabled for RasterFrames.
        """
        return RFContext.active()._jvm

