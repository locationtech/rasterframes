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
import sys
from . import RFContext

__all__ = ["create_rf_spark_session", "find_pyrasterframes_jar_dir", "find_pyrasterframes_assembly", "gdal_version"]


def find_pyrasterframes_jar_dir():
    """
    Locates the directory where JVM libraries for Spark are stored.
    :return: path to jar director as a string
    """
    jar_dir = None

    if sys.version < "3":
        import imp
        try:
            module_home = imp.find_module("pyrasterframes")[1]  # path
            jar_dir = os.path.join(module_home, 'jars')
        except ImportError:
            pass

    else:
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
        jar_dir = os.path.join(target_dir, 'scala-2.11')

    return os.path.realpath(jar_dir)


def find_pyrasterframes_assembly():
    jar_dir = find_pyrasterframes_jar_dir()
    jarpath = glob.glob(os.path.join(jar_dir, 'pyrasterframes-assembly*.jar'))

    if not len(jarpath) == 1:
        raise RuntimeError("""
Expected to find exactly one assembly. Found '{}' instead. 
Try running 'sbt pyrasterframes/clean pyrasterframes/package' first. """.format(jarpath))
    return jarpath[0]


def create_rf_spark_session(master="local[*]", **kwargs):
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

    try:
        spark.withRasterFrames()
        return spark
    except TypeError as te:
        print("Error setting up SparkSession; cannot find the pyrasterframes assembly jar\n", te)
        return None


def gdal_version():
    fcn = RFContext.active().lookup("buildInfo")
    return fcn()["GDAL"]
