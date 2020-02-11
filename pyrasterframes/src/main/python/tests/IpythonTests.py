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

from unittest import skip


import pyrasterframes
import pyrasterframes.rf_ipython
from pyrasterframes.rasterfunctions import *
from pyrasterframes.rf_types import *

from IPython.display import display_markdown
from IPython.display import display_html

import numpy as np

from py4j.protocol import Py4JJavaError
from . import TestEnvironment

class IpythonTests(TestEnvironment):

    def setUp(self):
        self.create_layer()

    @skip("Pending fix for issue #458")
    def test_all_nodata_tile(self):
        # https://github.com/locationtech/rasterframes/issues/458

        from pyspark.sql.types import StructType, StructField

        from pyspark.sql import Row
        df = self.spark.createDataFrame([
            Row(
                tile=Tile(np.array([[np.nan, np.nan, np.nan], [np.nan, np.nan, np.nan]], dtype='float64'),
                          CellType.float64())
            ),
            Row(tile=None)
        ], schema=StructType([StructField('tile', TileUDT(), True)]))

        try:
            pyrasterframes.rf_ipython.spark_df_to_html(df)
        except Py4JJavaError:
            self.fail("test_all_nodata_tile failed with Py4JJavaError")
        except:
            self.fail("um")
