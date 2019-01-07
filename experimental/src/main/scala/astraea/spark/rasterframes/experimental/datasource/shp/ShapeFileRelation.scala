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

package astraea.spark.rasterframes.experimental.datasource.shp
import java.net.URL

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.geotools.data.FileDataStore
import org.geotools.data.shapefile._
import org.geotools.data.simple._
import org.opengis.feature.`type`.AttributeType
import org.opengis.feature.simple._

import scala.collection.JavaConverters._

/**
 * Spark Datasource over a shapefile.
 *
 * @since 2019-01-05
 */
@Experimental
case class ShapeFileRelation(sqlContext: SQLContext, path: String)
    extends BaseRelation with TableScan {
  import ShapeFileRelation._

  /** Parsed form of path. */
  private val src = new URL(path)

  /** Invoke safe operation on DataStore. */
  private def withDatastore[A](f: FileDataStore => A): A = {
    val ds = new ShapefileDataStore(src)
    try {
      f(ds)
    } finally {
      ds.dispose()
    }
  }

  /** Invoke safe operation over simple feature iterator. */
  private def withFeatures[A](f: SimpleFeatureIterator => A): A = {
    withDatastore { ds =>
      val features = ds.getFeatureSource.getFeatures.features()
      try {
        f(features)
      } finally {
        features.close()
      }
    }
  }

  override def schema: StructType = withDatastore(ds => sft2catalyst(ds.getSchema))

  override def buildScan(): RDD[Row] = {
    var rows = Seq.empty[Row]
    withFeatures { features =>
      while (features.hasNext) {
        val sft = features.next()
        val row = Row(sft.getID +: sft.getAttributes.asScala: _*)
        rows = rows :+ row
      }
    }
    sqlContext.sparkContext.makeRDD(rows)
  }
}

object ShapeFileRelation {
  def sft2catalyst(at: AttributeType): DataType = {
    at.getBinding.getSimpleName match {
      case "String"             => StringType
      case "Integer"            => IntegerType
      case "Double"             => DoubleType
      case "Byte"               => ByteType
      case "Short"              => ShortType
      case "Float"              => FloatType
      case "Long"               => LongType
      case "Point"              => JTSTypes.PointTypeInstance
      case "Line"               => JTSTypes.LineStringTypeInstance
      case "Polygon"            => JTSTypes.PolygonTypeInstance
      case "MultiPoint"         => JTSTypes.MultiPointTypeInstance
      case "MultiLineString"    => JTSTypes.MultiLineStringTypeInstance
      case "MultiPolygon"       => JTSTypes.MultipolygonTypeInstance
      case "Geometry"           => JTSTypes.GeometryTypeInstance
      case "GeometryCollection" => JTSTypes.GeometryCollectionTypeInstance
    }
  }

  def sft2catalyst(name: String): String = name match {
    case "the_geom" => "geometry"
    case o => o.toLowerCase
  }

  def sft2catalyst(sft: SimpleFeatureType): StructType =
    StructType(
      StructField("id", StringType, false) +: {
        for (attr <- sft.getAttributeDescriptors.asScala) yield {
          StructField(sft2catalyst(attr.getLocalName), sft2catalyst(attr.getType))
        }
      }
    )
}
