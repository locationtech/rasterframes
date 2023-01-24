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

import builtins
import os

import pytest

from pyrasterframes.rasterfunctions import rf_convert_cell_type
from pyrasterframes.utils import create_rf_spark_session


# Setuptools/easy_install doesn't properly set the execute bit on the Spark scripts,
# So this preemptively attempts to do it.
def _chmodit():
    try:
        from importlib.util import find_spec

        module_home = find_spec("pyspark").origin
        print(module_home)
        bin_dir = os.path.join(os.path.dirname(module_home), "bin")
        for filename in os.listdir(bin_dir):
            try:
                os.chmod(os.path.join(bin_dir, filename), mode=0o555, follow_symlinks=True)
            except OSError:
                pass
    except ImportError:
        pass


_chmodit()


@pytest.fixture(scope="session")
def app_name():
    return "PyRasterFrames test suite"


@pytest.fixture(scope="session")
def resource_dir():
    here = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(here, "resources")


@pytest.fixture(scope="session")
def spark(app_name):
    spark_session = create_rf_spark_session(
        **{
            "spark.master": "local[*, 2]",
            "spark.ui.enabled": "false",
            "spark.app.name": app_name,
            #'spark.driver.extraJavaOptions': '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005'
        }
    )
    spark_session.sparkContext.setLogLevel("ERROR")

    print("Spark Version: " + spark_session.version)
    print("Spark Config: " + str(spark_session.sparkContext._conf.getAll()))

    return spark_session


@pytest.fixture()
def img_uri(resource_dir):
    img_path = os.path.join(resource_dir, "L8-B8-Robinson-IL.tiff")
    return "file://" + img_path


@pytest.fixture()
def img_rgb_uri(resource_dir):
    img_rgb_path = os.path.join(resource_dir, "L8-B4_3_2-Elkton-VA.tiff")
    return "file://" + img_rgb_path


@pytest.fixture()
def rf(spark, img_uri):
    # load something into a rasterframe
    rf = spark.read.geotiff(img_uri).with_bounds().with_center()

    # convert the tile cell type to provide for other operations
    return (
        rf.withColumn("tile2", rf_convert_cell_type("tile", "float32"))
        .drop("tile")
        .withColumnRenamed("tile2", "tile")
        .as_layer()
    )


@pytest.fixture()
def prdf(spark, img_uri):
    return spark.read.raster(img_uri)


@pytest.fixture()
def df(prdf):
    return prdf.withColumn("tile", rf_convert_cell_type("proj_raster", "float32")).drop(
        "proj_raster"
    )


def assert_png(bytes):
    assert (
        bytes[0:8] == bytearray([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),
        "png header does not match",
    )


def rounded_compare(val1, val2):
    print("Comparing {} and {} using round()".format(val1, val2))
    return builtins.round(val1) == builtins.round(val2)
