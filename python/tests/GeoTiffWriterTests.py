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

import os
import tempfile

import pytest
import rasterio


@pytest.fixture
def tmpfile():
    file_name = os.path.join(tempfile.gettempdir(), "pyrf-test.tif")
    yield file_name
    os.remove(file_name)


def test_identity_write(spark, img_uri, tmpfile):
    rf = spark.read.geotiff(img_uri)
    rf_count = rf.count()
    assert rf_count > 0

    rf.write.geotiff(tmpfile)
    rf2 = spark.read.geotiff(tmpfile)
    assert rf2.count() == rf.count()


def test_unstructured_write(spark, img_uri, tmpfile):
    rf = spark.read.raster(img_uri)

    rf.write.geotiff(tmpfile, crs="EPSG:32616")

    rf2 = spark.read.raster(tmpfile)

    assert rf2.count() == rf.count()

    with rasterio.open(img_uri) as source:
        with rasterio.open(tmpfile) as dest:
            assert (dest.width, dest.height) == (source.width, source.height)
            assert dest.bounds == source.bounds
            assert dest.crs == source.crs


def test_unstructured_write_schemaless(spark, img_uri, tmpfile):
    # should be able to write a projected raster tile column to path like '/data/foo/file.tif'
    from pyrasterframes.rasterfunctions import rf_agg_stats, rf_crs

    rf = spark.read.raster(img_uri)
    max = rf.agg(rf_agg_stats("proj_raster").max.alias("max")).first()["max"]
    crs = rf.select(rf_crs("proj_raster").alias("crs")).first()["crs"]

    assert not tmpfile.startswith("file://")

    rf.write.geotiff(tmpfile, crs=crs)

    with rasterio.open(tmpfile) as src:
        assert src.read().max() == max


def test_downsampled_write(spark, img_uri, tmpfile):
    rf = spark.read.raster(img_uri)
    rf.write.geotiff(tmpfile, crs="EPSG:32616", raster_dimensions=(128, 128))

    with rasterio.open(tmpfile) as f:
        assert (f.width, f.height) == (128, 128)
