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

import glob
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from . import RFContext
from typing import Union, Dict

__all__ = ["create_rf_spark_session", "find_pyrasterframes_jar_dir", "find_pyrasterframes_assembly", "gdal_version", 'is_notebook', 'gdal_version', 'build_info', 'quiet_logs']


def find_pyrasterframes_jar_dir() -> str:
    """
    Locates the directory where JVM libraries for Spark are stored.
    :return: path to jar director as a string
    """
    jar_dir = None

    from importlib.util import find_spec
    try:
        module_home = find_spec("pyrasterframes").origin
        jar_dir = os.path.join(os.path.dirname(module_home), 'jars')
    except ImportError:
        pass

    # Case for when we're running from source build
    if jar_dir is None or not os.path.exists(jar_dir):
        def pdir(curr):
            return os.path.dirname(curr)

        here = pdir(os.path.realpath(__file__))
        target_dir = pdir(pdir(here))
        # See if we're running outside of sbt build and adjust
        if os.path.basename(target_dir) != "target":
            target_dir = os.path.join(pdir(pdir(target_dir)), 'target')
        jar_dir = os.path.join(target_dir, 'scala-2.12')

    return os.path.realpath(jar_dir)


def find_pyrasterframes_assembly() -> Union[bytes, str]:
    jar_dir = find_pyrasterframes_jar_dir()
    jarpath = glob.glob(os.path.join(jar_dir, 'pyrasterframes-assembly*.jar'))

    if not len(jarpath) == 1:
        raise RuntimeError("""
Expected to find exactly one assembly. Found '{}' instead. 
Try running 'sbt pyrasterframes/clean pyrasterframes/package' first. """.format(jarpath))
    return jarpath[0]


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("geotrellis.raster.gdal").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def create_rf_spark_session(master="local[*]", **kwargs: str) -> SparkSession:
    """ Create a SparkSession with pyrasterframes enabled and configured. """
    jar_path = find_pyrasterframes_assembly()

    if 'spark.jars' in kwargs.keys():
        if 'pyrasterframes' not in kwargs['spark.jars']:
            raise UserWarning("spark.jars config is set, but it seems to be missing the pyrasterframes assembly jar.")

    conf = SparkConf().setAll([(k, kwargs[k]) for k in kwargs])

    spark = (SparkSession.builder
             .master(master)
             .appName("RasterFrames")
             .config('spark.jars', jar_path)
             .withKryoSerialization()
             .config(conf=conf)  # user can override the defaults
             .getOrCreate())

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
