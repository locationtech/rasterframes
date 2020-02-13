/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.expressions

import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}
import org.locationtech.jts.geom.Envelope
import org.locationtech.rasterframes.TestEnvironment
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.DynamicExtractorsSpec.{SnowflakeExtent1, SnowflakeExtent2}
import org.scalatest.Inspectors

class DynamicExtractorsSpec  extends TestEnvironment with Inspectors {
  describe("Extent extraction") {
    val expected = Extent(1, 2, 3, 4)
    it("should handle normal Extent") {
      extentExtractor.isDefinedAt(schemaOf[Extent]) should be(true)

      val row = expected.toInternalRow
      extentExtractor(schemaOf[Extent])(row) should be (expected)
    }
    it("should handle Envelope") {
      extentExtractor.isDefinedAt(schemaOf[Envelope]) should be(true)

      val e = expected.jtsEnvelope

      val row = e.toInternalRow
      extentExtractor(schemaOf[Envelope])(row) should be (expected)
    }

    it("should handle artisanally constructed Extents") {
      // Tests the case where PySpark will reorder manually constructed fields.
      // See https://stackoverflow.com/questions/35343525/how-do-i-order-fields-of-my-row-objects-in-spark-python/35343885#35343885

      import spark.implicits._
      withClue("case 1"){
        val special = SnowflakeExtent1(expected.xmax, expected.ymin, expected.xmin, expected.ymax)
        val df = Seq(Tuple1(special)).toDF("extent")
        val encodedType = df.schema.fields(0).dataType
        val encodedRow = SnowflakeExtent1.enc.toRow(special)
        extentExtractor.isDefinedAt(encodedType) should be(true)
        extentExtractor(encodedType)(encodedRow) should be(expected)
      }

      withClue("case 2") {
        val special = SnowflakeExtent2(expected.xmax, expected.ymin, expected.xmin, expected.ymax)
        val df = Seq(Tuple1(special)).toDF("extent")
        val encodedType = df.schema.fields(0).dataType
        val encodedRow = SnowflakeExtent2.enc.toRow(special)
        extentExtractor.isDefinedAt(encodedType) should be(true)
        extentExtractor(encodedType)(encodedRow) should be(expected)
      }
    }
  }

}

object DynamicExtractorsSpec {
  case class SnowflakeExtent1(xmax: Double, ymin: Double, xmin: Double, ymax: Double)

  object SnowflakeExtent1 {
    implicit val enc: ExpressionEncoder[SnowflakeExtent1] = Encoders.product[SnowflakeExtent1].asInstanceOf[ExpressionEncoder[SnowflakeExtent1]]
  }

  case class SnowflakeExtent2(xmax: Double, ymin: Double, xmin: Double, ymax: Double)

  object SnowflakeExtent2 {
    implicit val enc: ExpressionEncoder[SnowflakeExtent2] = Encoders.product[SnowflakeExtent2].asInstanceOf[ExpressionEncoder[SnowflakeExtent2]]
  }

}
