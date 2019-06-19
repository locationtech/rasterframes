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
import os
import unittest

from pyspark.sql import SparkSession
from pyrasterframes.rasterfunctions import *

import sys

if sys.version_info[0] > 2:
    import builtins
else:
    import __builtin__ as builtins


class TestEnvironment(unittest.TestCase):
    """
    Base class for tests.
    """

    def rounded_compare(self, val1, val2):
        print('Comparing {} and {} using round()'.format(val1, val2))
        return builtins.round(val1) == builtins.round(val2)

    @classmethod
    def setUpClass(cls):
        def pdir(curr):
            return os.path.dirname(curr)
        # gather Scala requirements
        here = pdir(os.path.realpath(__file__))
        target_dir = pdir(pdir(here))
        # See if we're running outside of sbt build and adjust
        if os.path.basename(target_dir) != "target":
            target_dir = os.path.join(pdir(pdir(target_dir)), 'target')
        scala_target = os.path.realpath(os.path.join(target_dir, 'scala-2.11'))

        jarpath = glob.glob(os.path.join(scala_target, 'pyrasterframes-assembly*.jar'))

        if not len(jarpath) == 1:
            raise RuntimeError("""
Expected to find exactly one assembly. Found '{}' instead. 
Try running 'sbt pyrasterframes/clean' first.""".format(jarpath))

        # hard-coded relative path for resources
        cls.resource_dir = os.path.realpath(os.path.join(scala_target, 'test-classes'))

        # spark session with RF
        cls.spark = (SparkSession.builder
                     .config('spark.driver.extraClassPath', jarpath[0])
                     .config('spark.executor.extraClassPath', jarpath[0])
                     .config('spark.ui.enabled', False)
                     .withKryoSerialization()
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

        print("Spark Version: " + cls.spark.version)
        print(cls.spark.sparkContext._conf.getAll())

        cls.spark.withRasterFrames()

        cls.img_uri = 'file://' + os.path.join(cls.resource_dir, 'L8-B8-Robinson-IL.tiff')

    def create_rasterframe(self):
        # load something into a rasterframe
        rf = self.spark.read.geotiff(self.img_uri) \
            .withBounds() \
            .withCenter()

        # convert the tile cell type to provide for other operations
        self.rf = rf.withColumn('tile2', rf_convert_cell_type('tile', 'float32')) \
            .drop('tile') \
            .withColumnRenamed('tile2', 'tile').asRF()
        # cls.rf.show()