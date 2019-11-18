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

from . import TestEnvironment

from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *

from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import *

import unittest


class ExploderTests(TestEnvironment):

    def test_no_data_filter_read_write(self):
        path = 'test_no_data_filter_read_write.pipe'
        df = self.spark.read.raster(self.img_uri) \
            .select(rf_tile_mean('proj_raster').alias('mean'))

        ndf = NoDataFilter().setInputCols(['mean'])
        assembler = VectorAssembler().setInputCols(['mean'])

        pipe = Pipeline().setStages([ndf, assembler])

        pipe.fit(df).write().overwrite().save(path)

        read_pipe = PipelineModel.load(path)
        self.assertEqual(len(read_pipe.stages), 2)
