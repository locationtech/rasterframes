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

from pyrasterframes.utils import create_rf_spark_session

import sys

if sys.version_info[0] > 2:
    import builtins
else:
    import __builtin__ as builtins

app_name = 'pyrasterframes test suite'

def resource_dir():
    def pdir(curr):
        return os.path.dirname(curr)

    here = os.path.dirname(os.path.realpath(__file__))
    scala_target = os.path.realpath(os.path.join(pdir(pdir(here)), 'scala-2.11'))
    rez_dir = os.path.realpath(os.path.join(scala_target, 'test-classes'))
    # If not running in build mode, try source dirs.
    if not os.path.exists(rez_dir):
        rez_dir = os.path.realpath(os.path.join(pdir(pdir(pdir(here))), 'test', 'resources'))
    return rez_dir


def spark_test_session():
    spark = create_rf_spark_session(**{
        'spark.ui.enabled': 'false',
        'spark.app.name': app_name
    })
    spark.sparkContext.setLogLevel('ERROR')

    print("Spark Version: " + spark.version)
    print("Spark Config: " + str(spark.sparkContext._conf.getAll()))

    return spark


class TestEnvironment(unittest.TestCase):
    """
    Base class for tests.
    """

    def rounded_compare(self, val1, val2):
        print('Comparing {} and {} using round()'.format(val1, val2))
        return builtins.round(val1) == builtins.round(val2)

    @classmethod
    def setUpClass(cls):
        # hard-coded relative path for resources
        cls.resource_dir = resource_dir()

        cls.spark = spark_test_session()

        cls.img_path = os.path.join(cls.resource_dir, 'L8-B8-Robinson-IL.tiff')

        cls.img_uri = 'file://' + cls.img_path

    @classmethod
    def l8band_uri(cls, band_index):
        return 'file://' + os.path.join(cls.resource_dir, 'L8-B{}-Elkton-VA.tiff'.format(band_index))

    def create_layer(self):
        from pyrasterframes.rasterfunctions import rf_convert_cell_type
        # load something into a rasterframe
        rf = self.spark.read.geotiff(self.img_uri) \
            .with_bounds() \
            .with_center()

        # convert the tile cell type to provide for other operations
        self.rf = rf.withColumn('tile2', rf_convert_cell_type('tile', 'float32')) \
            .drop('tile') \
            .withColumnRenamed('tile2', 'tile').as_layer()
