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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.gt.types

/**
 * Object responsible for registering functions with Catalyst
 *
 * @author sfitch
 * @since 4/12/17
 */
private[rasterframes] object Registrator {
  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rf_makeConstantTile", makeConstantTile)
    sqlContext.udf.register("rf_tileToArrayInt", tileToArray[Int])
    sqlContext.udf.register("rf_tileToArrayDouble", tileToArray[Double])
    sqlContext.udf.register("rf_aggHistogram", aggHistogram)
    sqlContext.udf.register("rf_aggStats", aggStats)
    sqlContext.udf.register("rf_tileMin", tileMean)
    sqlContext.udf.register("rf_tileMax", tileMean)
    sqlContext.udf.register("rf_tileMean", tileMean)
    sqlContext.udf.register("rf_tileSum", tileSum)
    sqlContext.udf.register("rf_tileHistogram", tileHistogram)
    sqlContext.udf.register("rf_tileStats", tileStats)
    sqlContext.udf.register("rf_dataCells", dataCells)
    sqlContext.udf.register("rf_nodataCells", dataCells)
    sqlContext.udf.register("rf_localAggStats", localAggStats)
    sqlContext.udf.register("rf_localAggMax", localAggMax)
    sqlContext.udf.register("rf_localAggMin", localAggMin)
    sqlContext.udf.register("rf_localAggMean", localAggMean)
    sqlContext.udf.register("rf_localAggCount", localAggCount)
    sqlContext.udf.register("rf_localAdd", localAdd)
    sqlContext.udf.register("rf_localSubtract", localSubtract)
    sqlContext.udf.register("rf_localMultiply", localMultiply)
    sqlContext.udf.register("rf_localDivide", localDivide)
    sqlContext.udf.register("rf_cellTypes", types.cellTypes)
    sqlContext.udf.register("rf_renderAscii", renderAscii)
  }
}
