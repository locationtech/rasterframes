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
import java.net.{URI, URL}
import java.nio.file.Paths
import java.util.zip.{ZipFile, ZipInputStream}

import astraea.spark.rasterframes.util._
import geotrellis.util.Filesystem
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.geotools.data.FileDataStore
import org.geotools.data.shapefile._
import org.geotools.data.simple._
import org.opengis.feature.`type`.AttributeType
import org.opengis.feature.simple._

import scala.collection.JavaConverters._

/**
 *
 *
 * @since 2019-01-05
 */
@Experimental
class ShapefileDataSource extends DataSourceRegister with RelationProvider {
  import ShapefileDataSource._
  override def shortName(): String = SHORT_NAME
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(
      PATH_PARAM,
      throw new IllegalArgumentException("Valid URI 'path' parameter required."))
    ShapeFileRelation(sqlContext, path)
  }
}
object ShapefileDataSource {
  final val SHORT_NAME = "shp"
  final val PATH_PARAM = "path"

  case class ShapeFileRelation(sqlContext: SQLContext, path: String)
      extends BaseRelation with TableScan {

    import ShapeFileRelation._

    /**
     * The shapefile basename is not likely the same as the zip file basename,
     * so we iterate over the contents of the zip file and assume the first '*.shp'
     * file we find represents the base name of the collection of files defining
     * the shapefile.
     */
    def lookupShp(provided: URL): String = {
      withResource(provided.openStream()) { input =>
        var name: String = null
        withResource(new ZipInputStream(input)) { zip =>
          while (name == null) {
            val entry = zip.getNextEntry
            val en = entry.getName
            if (!en.startsWith("_") && !en.startsWith(".") && en.endsWith(".shp")) {
              name = en
            }
          }
        }

        if (name != null) name
        // Fall back on the basename of the zip file.
        else Paths.get(provided.getPath).getFileName.toString
      }
    }

    /** Parsed form of path. */
    private val src = {
      val source = new URL(path)
      if (path.endsWith(".zip")) {
        val filename = lookupShp(source)
        // Turns out that the JVM's built-in URL connection handler
        // will read zip file contents when using the following syntax, and
        // GeoTools Shapefile parser plays nicely with it in searching for
        // sidecar files.
        new URL(s"jar:${ source}!/$filename")
      }
      else source
    }

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

    /** Read and convert the schema in the shapefile. */
    override def schema: StructType = withDatastore(ds => sft2catalyst(ds.getSchema))

    /** Convert the contents of the shapefile into rows. */
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
    private def sft2catalyst(at: AttributeType): DataType = {
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

    private def sft2catalyst(name: String): String = name match {
      case "the_geom" => "geometry"
      case o          => o.toLowerCase
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
}
