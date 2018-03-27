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

package org.apache.spark.sql.gt.types

import geotrellis.raster.histogram.Histogram
import org.apache.spark.sql.rf.KryoBackedUDT
import org.apache.spark.sql.types._

/**
 * Wraps up GT Histogram type.
 *
 * @since 4/18/17
 */
class HistogramUDT extends UserDefinedType[Histogram[Double]] with KryoBackedUDT[Histogram[Double]] {

  override val typeName = "gt_histogram"

  override val targetClassTag = scala.reflect.classTag[Histogram[Double]]

  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case o: HistogramUDT ⇒ o.typeName == this.typeName
    case _ ⇒ super.acceptsType(dataType)
  }
}

object HistogramUDT extends HistogramUDT {
  UDTRegistration.register(classOf[Histogram[Double]].getName, classOf[HistogramUDT].getName)
}
