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

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *

from pyrasterframes import TileExploder
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *


def test_tile_exploder_pipeline_for_prt(spark, img_uri):
    # NB the tile is a Projected Raster Tile
    df = spark.read.raster(img_uri)
    t_col = "proj_raster"
    assert t_col in df.columns, "proj_raster column not found"

    assembler = VectorAssembler().setInputCols([t_col])
    pipe = Pipeline().setStages([TileExploder(), assembler])
    pipe_model = pipe.fit(df)
    tranformed_df = pipe_model.transform(df)
    assert tranformed_df.count() > df.count(), "DF count has not the expected size"


def test_tile_exploder_pipeline_for_tile(spark, img_uri):
    t_col = "tile"
    df = spark.read.raster(img_uri).withColumn(t_col, rf_tile("proj_raster")).drop("proj_raster")

    assembler = VectorAssembler().setInputCols([t_col])
    pipe = Pipeline().setStages([TileExploder(), assembler])
    pipe_model = pipe.fit(df)
    tranformed_df = pipe_model.transform(df)
    assert tranformed_df.count() > df.count(), "DF count has not the expected size"


def test_tile_exploder_read_write(spark, img_uri):
    path = "test_tile_exploder_read_write.pipe"
    df = spark.read.raster(img_uri)

    assembler = VectorAssembler().setInputCols(["proj_raster"])
    pipe = Pipeline().setStages([TileExploder(), assembler])

    pipe.fit(df).write().overwrite().save(path)

    read_pipe = PipelineModel.load(path)
    assert len(read_pipe.stages) == 2
    assert isinstance(read_pipe.stages[0], TileExploder)
