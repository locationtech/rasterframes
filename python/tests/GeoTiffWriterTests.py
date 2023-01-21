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
        return os.path.join(tempfile.gettempdir(), "pyrf-test.tif")

    def test_identity_write(self):
        rf = self.spark.read.geotiff(self.img_uri)
        rf_count = rf.count()
        self.assertTrue(rf_count > 0)

        dest = self._tmpfile()
        rf.write.geotiff(dest)

        rf2 = self.spark.read.geotiff(dest)

        self.assertEqual(rf2.count(), rf.count())

        os.remove(dest)

    def test_unstructured_write(self):
        rf = self.spark.read.raster(self.img_uri)
        dest_file = self._tmpfile()
        rf.write.geotiff(dest_file, crs='EPSG:32616')

        rf2 = self.spark.read.raster(dest_file)
        self.assertEqual(rf2.count(), rf.count())

        with rasterio.open(self.img_uri) as source:
            with rasterio.open(dest_file) as dest:
                self.assertEqual((dest.width, dest.height), (source.width, source.height))
                self.assertEqual(dest.bounds, source.bounds)
                self.assertEqual(dest.crs, source.crs)

        os.remove(dest_file)

    def test_unstructured_write_schemaless(self):
        # should be able to write a projected raster tile column to path like '/data/foo/file.tif'
        from pyrasterframes.rasterfunctions import rf_agg_stats, rf_crs
        rf = self.spark.read.raster(self.img_uri)
        max = rf.agg(rf_agg_stats('proj_raster').max.alias('max')).first()['max']
        crs = rf.select(rf_crs('proj_raster').alias('crs')).first()['crs']

        dest_file = self._tmpfile()
        self.assertTrue(not dest_file.startswith('file://'))
        rf.write.geotiff(dest_file, crs=crs)

        with rasterio.open(dest_file) as src:
            self.assertEqual(src.read().max(), max)

        os.remove(dest_file)

    def test_downsampled_write(self):
        rf = self.spark.read.raster(self.img_uri)
        dest = self._tmpfile()
        rf.write.geotiff(dest, crs='EPSG:32616', raster_dimensions=(128, 128))

        with rasterio.open(dest) as f:
            self.assertEqual((f.width, f.height), (128, 128))

        os.remove(dest)

