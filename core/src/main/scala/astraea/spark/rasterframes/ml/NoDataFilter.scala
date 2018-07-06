/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package astraea.spark.rasterframes.ml

import astraea.spark.rasterframes.ml.Parameters.HasInputCols
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import java.util.ArrayList
import scala.collection.JavaConversions._

/**
 * Transformer filtering out rows containing NoData/NA values in
 * any of the specified columns.
 *
 * @since 9/21/17
 */
class NoDataFilter (override val uid: String) extends Transformer
  with HasInputCols with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("nodata-filter"))
  final def setInputCols(value: Array[String]): NoDataFilter = set(inputCols, value)
  final def setInputCols(values: ArrayList[String]): this.type = {
    val valueArr = values.toArray[String]
    set(inputCols, valueArr)
  }

  setInputCols(Array("tile"))

  override def copy(extra: ParamMap): NoDataFilter = defaultCopy(extra)

  def transform(dataset: Dataset[_]) =
    dataset.na.drop(getInputCols.intersect(dataset.columns))

  def transformSchema(schema: StructType) = schema
}

object NoDataFilter extends DefaultParamsReadable[NoDataFilter]
