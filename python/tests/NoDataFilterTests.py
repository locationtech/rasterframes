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


from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *


def test_no_data_filter_read_write(spark, img_uri):
    path = "test_no_data_filter_read_write.pipe"
    df = spark.read.raster(img_uri).select(rf_tile_mean("proj_raster").alias("mean"))

    input_cols = ["mean"]
    ndf = NoDataFilter().setInputCols(input_cols)
    assembler = VectorAssembler().setInputCols(input_cols)

    pipe = Pipeline().setStages([ndf, assembler])

    pipe.fit(df).write().overwrite().save(path)

    read_pipe = PipelineModel.load(path)
    assert len(read_pipe.stages) == 2
    actual_stages_ndf = read_pipe.stages[0].getInputCols()
    assert actual_stages_ndf == input_cols
