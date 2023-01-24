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


import numpy as np
import pytest
from IPython.testing import globalipapp
from py4j.protocol import Py4JJavaError
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

import pyrasterframes
from pyrasterframes.rf_types import *


@pytest.fixture(scope="module")
def ip():
    globalipapp.start_ipython()
    yield globalipapp.get_ipython()
    globalipapp.get_ipython().atexit_operations()


@pytest.mark.skip("Pending fix for issue #458")
def test_all_nodata_tile(spark):
    # https://github.com/locationtech/rasterframes/issues/458

    df = spark.createDataFrame(
        [
            Row(
                tile=Tile(
                    np.array([[np.nan, np.nan, np.nan], [np.nan, np.nan, np.nan]], dtype="float64"),
                    CellType.float64(),
                )
            ),
            Row(tile=None),
        ],
        schema=StructType([StructField("tile", TileUDT(), True)]),
    )

    try:
        pyrasterframes.rf_ipython.spark_df_to_html(df)
    except Py4JJavaError:
        raise Exception("test_all_nodata_tile failed with Py4JJavaError")
    except:
        raise Exception("um")


def test_display_extension(ip, df):
    import pyrasterframes.rf_ipython

    num_rows = 2

    result = {}

    def counter(data, md):
        nonlocal result
        result["payload"] = (data, md)
        result["row_count"] = data.count("<tr>")

    ip.mime_renderers["text/html"] = counter

    # ip.mime_renderers['text/markdown'] = lambda a, b: print(a, b)

    df.display(num_rows=num_rows)

    # Plus one for the header row.
    assert result["row_count"] == num_rows + 1, f"Received: {result['payload']}"
