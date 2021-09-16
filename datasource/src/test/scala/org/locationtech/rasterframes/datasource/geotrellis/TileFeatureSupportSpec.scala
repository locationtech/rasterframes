/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.locationtech.rasterframes.datasource.geotrellis

import geotrellis.layer.LayoutDefinition
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.geotrellis.TileFeatureSupport._
import org.locationtech.rasterframes.util._
import geotrellis.proj4.LatLng
import geotrellis.raster.crop.Crop
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.resample.Bilinear
import geotrellis.raster._
import geotrellis.spark.tiling.Implicits._
import geotrellis.layer._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.rasterframes.TestEnvironment
import org.scalatest.BeforeAndAfter

import scala.reflect.ClassTag


class TileFeatureSupportSpec extends TestEnvironment
  with TestData
  with BeforeAndAfter {

  val strTF1 = TileFeature(squareIncrementingTile(3), List("data1"))
  val strTF2 = TileFeature(squareIncrementingTile(3), List("data2"))
  val ext1 = Extent(10,10,20,20)
  val ext2 = Extent(15,15,25,25)
  val cropOpts: Crop.Options = Crop.Options.DEFAULT
  val gb = GridBounds(0,0,1,1)
  val geoms = Seq(ext2.toPolygon())
  val maskOpts: Rasterizer.Options = Rasterizer.Options.DEFAULT


  describe("TileFeatureSupport") {
    it("should support merge, prototype operations") {

      val merged = strTF1.merge(strTF2)
      assert(merged.tile == strTF1.tile.merge(strTF2.tile))
      assert(merged.data == List("data1", "data2"))

      val proto = strTF1.prototype(16,16)
      assert(proto.tile == byteArrayTile.prototype(16,16))
      assert(proto.data == Nil)
    }

    it("should enable tileToLayout over TileFeature RDDs") {
      val peRDD = TileFeatureSupportSpec.randomProjectedExtentTileFeatureRDD(100)
      val layout = LayoutDefinition(LatLng.worldExtent,TileLayout(5,5,40,40))

      val newRDD = peRDD.tileToLayout(ShortConstantNoDataCellType,layout)
      assert(newRDD.count == 25) // assumes peRDD covers full layout

      val firstTF = newRDD.first._2
      assert(firstTF.tile.rows == 40)
      assert(firstTF.tile.cols == 40)
    }

    it("should support ops with custom data type") {

      case class Foo(num:Int,seq:Seq[String])

      implicit object FooOps extends MergeableData[Foo] {
        override def merge(l: Foo, r: Foo): Foo = Foo(l.num + r.num, l.seq ++ r.seq)
        override def prototype(data: Foo): Foo = Foo(0,Seq())
      }

      val foo1 = Foo(10,Seq("Hello","Goodbye"))
      val foo2 = Foo(20, Seq("HelloAgain"))
      val fooTF1 = TileFeature(byteArrayTile, foo1)
      val fooTF2 = TileFeature(byteArrayTile, foo2)

      testAllOps(fooTF1,fooTF2)
    }

    it("should support full ops with String data") {
      val strTF1 = TileFeature(squareIncrementingTile(3), "tf1")
      val strTF2 = TileFeature(squareIncrementingTile(3), "tf2")

      testAllOps(strTF1, strTF2)
    }

    it("should support full ops with Seq data") {
      val seqTF1 = TileFeature(squareIncrementingTile(3), Seq("tf1"))
      val seqTF2 = TileFeature(squareIncrementingTile(3), Seq("tf2"))

      testAllOps(seqTF1, seqTF2)
    }

    it("should support full ops with Set data") {
      val setTF1 = TileFeature(squareIncrementingTile(3), Set("foo", "bar"))
      val setTF2 = TileFeature(squareIncrementingTile(3), Set("foo", "ball"))
      testAllOps(setTF1, setTF2)
    }

    it("should support full ops with Map data") {
      // works for Map[String,String]
      val mapStrStrTF1 = TileFeature(squareIncrementingTile(3),Map("foo" -> "bar","hello" -> "goodbye"))
      val mapStrStrTF2 = TileFeature(squareIncrementingTile(3),Map("foo" -> "ball","slap" -> "shot"))
      testAllOps(mapStrStrTF1, mapStrStrTF2)

      // works for Map[Int,String]
      val mapIntStrTF1 = TileFeature(squareIncrementingTile(3),Map(1 -> "bar", 2 -> "two"))
      val mapIntStrTF2 = TileFeature(squareIncrementingTile(3),Map(1 -> "ball", 3 -> "three"))
      testAllOps(mapIntStrTF1, mapIntStrTF2)

      // Map[String,Seq[String]]
      val mapStrSeqStrTF1 = TileFeature(squareIncrementingTile(3),Map("foo" -> Seq("hello"),"bar" -> Seq("Goodbye")))
      val mapStrSeqStrTF2 = TileFeature(squareIncrementingTile(3),Map("foo" -> Seq("Hello"),"cat" -> Seq("Yo","Bonjour")))
      testAllOps(mapStrSeqStrTF1, mapStrSeqStrTF2)
    }
  }

  private def testAllOps[V <: CellGrid[Int]: ClassTag: WithMergeMethods: WithPrototypeMethods:
    WithCropMethods: WithMaskMethods, D: MergeableData](tf1: TileFeature[V, D], tf2: TileFeature[V, D]) = {
    
    assert(tf1.prototype(20, 20) == TileFeature(tf1.tile.prototype(20, 20), MergeableData[D].prototype(tf1.data)))
    assert(tf1.prototype(IntCellType, 20, 20) == TileFeature(tf1.tile.prototype(IntCellType, 20, 20), MergeableData[D].prototype(tf1.data)))

    assert(tf1.crop(ext1, ext2, cropOpts) == TileFeature(tf1.tile.crop(ext1, ext2, cropOpts), tf1.data))
    assert(tf1.crop(gb, cropOpts) == TileFeature(tf1.tile.crop(gb, cropOpts), tf1.data))

    assert(tf1.mask(ext1, geoms, maskOpts) == TileFeature(tf1.tile.mask(ext1, geoms, maskOpts), tf1.data))
    assert(tf1.localMask(tf2, 1, 2) == TileFeature(tf1.tile.localMask(tf2.tile, 1, 2), tf1.data))
    assert(tf1.localInverseMask(tf2, 1, 2) == TileFeature(tf1.tile.localInverseMask(tf2.tile, 1, 2), tf1.data))

    assert(tf1.merge(tf2) == TileFeature(tf1.tile.merge(tf2.tile), MergeableData[D].merge(tf1.data, tf2.data)))
    assert(tf1.merge(ext1, ext2, tf2, Bilinear) == TileFeature(tf1.tile.merge(ext1, ext2, tf2.tile, Bilinear), MergeableData[D].merge(tf1.data,tf2.data)))
  }
}


object TileFeatureSupportSpec {

  implicit class RichRandom(val rnd: scala.util.Random) extends AnyVal {
    def nextDouble(max: Double): Double = (rnd.nextInt * max) / Int.MaxValue.toDouble
    def nextOrderedPair(max:Double): (Double,Double) = (nextDouble(max),nextDouble(max)) match {
      case(l,r) if l > r => (r,l)
      case(l,r) if l == r => nextOrderedPair(max)
      case pair => pair
    }
  }

  def randomProjectedExtentTileRDD(n:Int)(implicit sc: SparkContext): RDD[(ProjectedExtent,Tile)] = {
    val rnd = new scala.util.Random(31415)
    sc.parallelize((1 to n).map(i => {
      val (latMin, latMax) = rnd.nextOrderedPair(90)
      val (lonMin, lonMax) = rnd.nextOrderedPair(180)
      (ProjectedExtent(Extent(lonMin, latMin, lonMax, latMax),LatLng),TestData.randomTile(20, 20, ShortCellType))
    }))
  }

  def randomProjectedExtentTileFeatureRDD(n:Int)(implicit sc: SparkContext): RDD[(ProjectedExtent, TileFeature[Tile,String])] = {
    val rnd = new scala.util.Random(112358)
    randomProjectedExtentTileRDD(n).mapValues(tile => TileFeature(tile,new String(rnd.alphanumeric.take(4).toArray)))
  }
}
