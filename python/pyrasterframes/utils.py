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

from typing import Dict, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

from . import RFContext

__all__ = [
    "create_rf_spark_session",
    "gdal_version",
    "gdal_version",
    "build_info",
    "quiet_logs",
]


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("geotrellis.raster.gdal").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def create_rf_spark_session(master="local[*]", **kwargs: str) -> Optional[SparkSession]:
    """
    Create a SparkSession with pyrasterframes enabled and configured.
    Expects pyrasterframes-assembly-x.x.x.jar in JarPath
    """
    conf = SparkConf().setAll([(k, kwargs[k]) for k in kwargs])

    spark = (
        SparkSession.builder.master(master)
        .appName("RasterFrames")
        .withKryoSerialization()
        .config(conf=conf)  # user can override the defaults
        .getOrCreate()
    )

    quiet_logs(spark)

    try:
        spark.withRasterFrames()
        return spark
    except TypeError as te:
        print("Error setting up SparkSession; cannot find the pyrasterframes assembly jar\n", te)
        return None


def gdal_version() -> str:
    fcn = RFContext.active().lookup("buildInfo")
    return fcn()["GDAL"]


def build_info() -> Dict[str, str]:
    fcn = RFContext.active().lookup("buildInfo")
    return fcn()
