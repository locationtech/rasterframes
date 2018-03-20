/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package astraea.spark.rasterframes.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.DataType

/**
 * Aggregation function that only returns the average. Depends on
 * [[LocalStatsAggregateFunction]] for computation and just
 * selects the mean result tile.
 *
 * @author sfitch
 * @since 8/11/17
 */
class LocalMeanAggregateFunction extends LocalStatsAggregateFunction {
  override def dataType: DataType = new TileUDT()
  override def evaluate(buffer: Row): Any = {
    val superRow = super.evaluate(buffer).asInstanceOf[Row]
    if (superRow != null) superRow.get(3) else null
  }
}
