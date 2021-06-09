/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

package org.locationtech.rasterframes.encoders

import cats.data
import geotrellis.proj4._
import geotrellis.raster.{CellSize, CellType, Dimensions, TileLayout, UShortUserDefinedNoDataCellType}
import geotrellis.layer._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.typeTag
import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, IsNull, KnownNotNull}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.locationtech.rasterframes.{TestData, TestEnvironment}
import org.locationtech.rasterframes.model.{CellContext, TileContext, TileDataContext}
import org.scalatest.Assertion


case class SpecialKey(z: Int, y: Int)
case class BoxBounds[T](a: T, b: T)

class DevSpec extends TestEnvironment {
  import TestData._

  describe("automatic derivation"){
  }

  describe("scalar tile operations") {
    import org.locationtech.rasterframes.tiles.ProjectedRasterTile

    import spark.implicits._

    ignore("=== DeSer for Time ===") {
      import org.apache.spark.sql.catalyst.ScalaReflection.deserializerForType
      import scala.reflect.runtime.universe._

      val de = deserializerForType(typeOf[java.sql.Timestamp])

      info(de.numberedTreeString)
    }

    ignore("does it really serialize CRS?") {
      val first = CRSEncoder()

      info("Before: \n" + first.deserializer.treeString)

      val en = first.resolveAndBind()
      info("After: \n" + en.deserializer.treeString)

      val ir = en.createSerializer().apply(LatLng)
      val out = en.createDeserializer().apply(ir)
      out shouldBe LatLng
    }

    it("prt encoder"){
      val en = ProjectedRasterTile.prtEncoder
      info(en.objSerializer.treeString)
      info(en.deserializer.treeString)
    }

    it("should get schema") {
      val dt = org.apache.spark.sql.catalyst.ScalaReflection.schemaFor[SpatialKey]
      info(dt.dataType.simpleString)
    }

    it("should round trip SpatialKey") {

      implicit val en = implicitly[Encoder[KeyBounds[SpatialKey]]]
      val data = Seq(KeyBounds(SpatialKey(45,42), SpatialKey(51,52)))
      val ds = data.toDS
      ds.printSchema()
      ds.show()
      val df = ds.toDF()

      val out = df.as[KeyBounds[SpatialKey]].first()
      info(out.toString)
    }

    it("should round trip ProjectedRasterTile") {
      val data = Seq(one, two)
      val ds = data.toDS
      ds.printSchema()
      ds.show()
      val df = ds.toDF()

      val tile = df.as[ProjectedRasterTile].first()
      info(tile.toString)
    }

    it("one") {
      type T = ProjectedRasterTile
      val data = Seq(one)
      val in: Encoder[T] = implicitly[Encoder[T]]
      val enc = encoderFor[T](in)
      val toRow = enc.createSerializer()
      val attributes = enc.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val encoded = data.map(d => toRow(d).copy())
      val plan = new LocalRelation(attributes, encoded)
      val dataset = new Dataset(spark, plan, implicitly[Encoder[T]])
      dataset.printSchema()
      dataset.show()
      val df = dataset.toDF("one")
      df.printSchema

      //      val df2 = localSeqToDatasetHolder(Seq(one)).toDF("one")
    }

    it("two") {
      type T = (ProjectedRasterTile, ProjectedRasterTile)
      val data = Seq((one, one))
      val in: Encoder[T] = implicitly[Encoder[T]]
      val enc = encoderFor[T](in)
      val toRow = enc.createSerializer()
      val attributes = enc.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val encoded = data.map(d => toRow(d).copy())
      val plan = new LocalRelation(attributes, encoded)
      val dataset = new Dataset(spark, plan, implicitly[Encoder[T]])
      dataset.printSchema()
      dataset.show()
      val df = dataset.toDF("one", "two")
      df.printSchema()

      //      val df1 = localSeqToDatasetHolder(Seq((one, one))).toDF("one","other")
    }

    it("should rf_local_add") {
      val df = Seq(two).toDF("one")
      val tile = df.as[ProjectedRasterTile].first()
      info(tile.toString)
      df.printSchema()
      df.show()

    }

    it("should handle two") {
      val df = Seq((one, two)).toDF("one", "two")
      df.printSchema()
      df.show()
      val tile = df.select("one").as[ProjectedRasterTile].first()
      info(tile.toString)
    }
  }

}
