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
import os
import sys

__all__ = ["create_rf_spark_session", "find_pyrasterframes_jar_dir", "find_pyrasterframes_assembly"]


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
Try running 'sbt pyrasterframes/package' first. """.format(jarpath))
    return jarpath[0]


def create_rf_spark_session():
    jar_path = find_pyrasterframes_jar_dir()

    jars_cp = ','.join([f.path for f in os.scandir(jar_path) if f.name[-3:] == 'jar'])

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("RasterFrames")
             .config('spark.jars', jars_cp)
             .config("spark.ui.enabled", "false")
             .withKryoSerialization()
             .getOrCreate())
    spark.withRasterFrames()
    return spark
