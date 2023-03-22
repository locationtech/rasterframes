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
import pathlib
import shutil
import tempfile

import pytest


@pytest.fixture()
def tmpdir():
    dest = tempfile.mkdtemp()
    yield pathlib.Path(dest).as_uri()
    shutil.rmtree(dest, ignore_errors=True)


def test_write_geotrellis_layer(spark, img_uri, tmpdir):
    rf = spark.read.geotiff(img_uri).cache()
    rf_count = rf.count()
    assert rf_count > 0

    layer = "gt_layer"
    zoom = 0

    rf.write.option("layer", layer).option("zoom", zoom).geotrellis(tmpdir)

    rf_gt = spark.read.format("geotrellis").option("layer", layer).option("zoom", zoom).load(tmpdir)
    rf_gt_count = rf_gt.count()
    assert rf_gt_count > 0

    _ = rf_gt.take(1)


def test_write_geotrellis_multiband_layer(spark, img_rgb_uri, tmpdir):
    rf = spark.read.geotiff(img_rgb_uri).cache()
    rf_count = rf.count()
    assert rf_count > 0

    layer = "gt_multiband_layer"
    zoom = 0

    rf.write.option("layer", layer).option("zoom", zoom).geotrellis(tmpdir)

    rf_gt = spark.read.format("geotrellis").option("layer", layer).option("zoom", zoom).load(tmpdir)
    rf_gt_count = rf_gt.count()
    assert rf_gt_count > 0

    _ = rf_gt.take(1)
