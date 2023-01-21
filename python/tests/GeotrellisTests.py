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
import os
import pathlib
import shutil
import tempfile
from unittest import skipIf

from . import TestEnvironment


class GeotrellisTests(TestEnvironment):

    on_circle_ci = os.environ.get("CIRCLECI", "false") == "true"

    @skipIf(
        on_circle_ci,
        "CircleCI has java.lang.NoClassDefFoundError fs2/Stream when taking action on rf_gt",
    )
    def test_write_geotrellis_layer(self):
        rf = self.spark.read.geotiff(self.img_uri).cache()
        rf_count = rf.count()
        self.assertTrue(rf_count > 0)

        layer = "gt_layer"
        zoom = 0

        dest = tempfile.mkdtemp()
        dest_uri = pathlib.Path(dest).as_uri()
        rf.write.option("layer", layer).option("zoom", zoom).geotrellis(dest_uri)

        rf_gt = (
            self.spark.read.format("geotrellis")
            .option("layer", layer)
            .option("zoom", zoom)
            .load(dest_uri)
        )
        rf_gt_count = rf_gt.count()
        self.assertTrue(rf_gt_count > 0)

        _ = rf_gt.take(1)

        shutil.rmtree(dest, ignore_errors=True)

    @skipIf(
        on_circle_ci,
        "CircleCI has java.lang.NoClassDefFoundError fs2/Stream when taking action on rf_gt",
    )
    def test_write_geotrellis_multiband_layer(self):
        rf = self.spark.read.geotiff(self.img_rgb_uri).cache()
        rf_count = rf.count()
        self.assertTrue(rf_count > 0)

        layer = "gt_multiband_layer"
        zoom = 0

        dest = tempfile.mkdtemp()
        dest_uri = pathlib.Path(dest).as_uri()
        rf.write.option("layer", layer).option("zoom", zoom).geotrellis(dest_uri)

        rf_gt = (
            self.spark.read.format("geotrellis")
            .option("layer", layer)
            .option("zoom", zoom)
            .load(dest_uri)
        )
        rf_gt_count = rf_gt.count()
        self.assertTrue(rf_gt_count > 0)

        _ = rf_gt.take(1)

        shutil.rmtree(dest, ignore_errors=True)
