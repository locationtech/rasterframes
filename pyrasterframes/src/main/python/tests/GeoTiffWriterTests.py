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

from . import TestEnvironment
import rasterio


class GeoTiffWriter(TestEnvironment):

    @staticmethod
    def _tmpfile():
        return os.path.join(tempfile.tempdir, "pyrf-test.tif")

    def test_identity_write(self):
        rf = self.spark.read.geotiff(self.img_uri)

        dest = self._tmpfile()
        rf.write.geotiff(dest)

        rf2 = self.spark.read.geotiff(dest)
        self.assertEqual(rf2.count(), rf.count())

        os.remove(dest)

    def test_unstructured_write(self):
        rf = self.spark.read.raster(self.img_uri)
        dest = self._tmpfile()
        rf.write.geotiff(dest, crs='EPSG:32616')

        rf2 = self.spark.read.raster(dest)
        self.assertEqual(rf2.count(), rf.count())

        os.remove(dest)

    def test_downsampled_write(self):
        rf = self.spark.read.raster(self.img_uri)
        dest = self._tmpfile()
        rf.write.geotiff(dest, crs='EPSG:32616', raster_dimensions=(128, 128))

        with rasterio.open(dest) as f:
            self.assertEqual((f.width, f.height), (128, 128))

        os.remove(dest)

