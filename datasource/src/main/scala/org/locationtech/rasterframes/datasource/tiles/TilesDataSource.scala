/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Astraea, Inc.
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
package org.locationtech.rasterframes.datasource.tiles

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags, Tiled}
import geotrellis.raster.render.ColorRamps
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.store.hadoop.{SerializableConfiguration, _}
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SQLContext, SaveMode, functions => F}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource._
import org.locationtech.rasterframes.encoders.SparkBasicEncoders
import org.locationtech.rasterframes.util._

import java.io.IOException
import java.net.URI
import scala.util.Try

class TilesDataSource extends DataSourceRegister with CreatableRelationProvider {
  import TilesDataSource._
  override def shortName(): String = SHORT_NAME

  /**
   * Credit: https://stackoverflow.com/a/50545815/296509
   */
  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }

  private def writeCatalog(pipeline: Dataset[Row], pathURI: URI, conf: SerializableConfiguration) = {
    // A bit of a hack here. First we write the CSV using Spark's CSV writer, then we clean up all the Hadoop noise.
    val fName = "catalog.csv"
    val hPath = new Path(new Path(pathURI), "_" + fName)
    pipeline
      .write
      .option("header", "true")
      .csv(hPath.toString)

    val fs = FileSystem.get(pathURI, conf.value)
    val localPath = new Path(new Path(pathURI), fName)
    copyMerge(fs, hPath, fs, localPath, true, conf.value)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val pathURI = parameters.path.getOrElse(throw new IllegalArgumentException("Valid URI 'path' parameter required."))
    require(pathURI.getScheme == "file" || pathURI.getScheme == null, "Currently only 'file://' destinations are supported")

    val tileCols = data.tileColumns
    require(tileCols.nonEmpty, "Could not find any tile columns.")

    val filenameCol = parameters.filenameColumn
      .map(F.col)
      .getOrElse(F.monotonically_increasing_id().cast(StringType))

    val SpatialComponents(crsCol, extentCol, _, _) = projectSpatialComponents(data) match {
      case Some(parts) => parts
      case _ => throw new IllegalArgumentException("Could not find extent and/or CRS data.")
    }

    val tags = Tags(Map.empty,
      tileCols.map(c => Map("source_column" -> c.columnName)).toList
    )

    // We make some assumptions here.... eventually have column metadata encode this.
    val colorSpace = tileCols.size match {
      case 3 | 4 => ColorSpace.RGB
      case _     => ColorSpace.BlackIsZero
    }

    val metadataCols = parameters.metadataColumns

    // Default format options.
    val tiffOptions = GeoTiffOptions(Tiled, DeflateCompression, colorSpace)

    val outRowEnc = RowEncoder(StructType(
      StructField("filename", StringType) +:
        StructField("bbox", StringType) +:
        StructField("crs", StringType) +:
        metadataCols.map(n =>
          StructField(n, StringType)
        )
    ))

    val hconf = SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration)

    // Spark ceremony for reifying row contents.
    import SparkBasicEncoders._
    val inRowEnc = Encoders.tuple(
      stringEnc, crsExpressionEncoder, extentEncoder, arrayEnc[Tile], arrayEnc[String])
    type RowStuff = (String, CRS, Extent, Array[Tile], Array[String])
    val pipeline = data
      .select(filenameCol, crsCol, extentCol, F.array(tileCols.map(rf_tile): _*),
        F.array(metadataCols.map(data.apply).map(_.cast(StringType)): _*))
      .na.drop()
      .as[RowStuff](inRowEnc)
      .mapPartitions { rows =>
        for ((filename, crs, extent, tiles, metadata) <- rows) yield {
          val md = metadataCols.zip(metadata).toMap

          val finalFilename = if (parameters.asPNG) {
            val fnl = filename.toLowerCase()
            if (!fnl.endsWith("png")) filename + ".png" else filename
          }
          else {
            val fnl = filename.toLowerCase()
            if (!(fnl.endsWith("tiff") || fnl.endsWith("tif"))) filename + ".tif" else filename
          }

          val finalPath = new Path(new Path(pathURI), finalFilename)

          if (parameters.asPNG) {
            // `Try` below is due to https://github.com/locationtech/geotrellis/issues/2621
            val scaled = tiles.map(t => Try(t.rescale(0, 255)).getOrElse(t))
            if (scaled.length > 1)
              MultibandTile(scaled).renderPng().write(finalPath, hconf.value)
            else
              scaled.head.renderPng(ColorRamps.greyscale(255)).write(finalPath, hconf.value)
          }
          else {
            val chipTags = tags.copy(headTags = md.updated("base_filename", filename))
            val geotiff = new MultibandGeoTiff(MultibandTile(tiles), extent, crs, chipTags, tiffOptions)
            geotiff.write(finalPath, hconf.value)
          }
          // Ordering:
          //   bbox = left,bottom,right,top
          //   bbox = min Longitude , min Latitude , max Longitude , max Latitude
          // Avoiding commas with this format:
          //   [0.489|51.28|0.236|51.686]
          val bbox = s"[${extent.xmin}|${extent.ymin}|${extent.xmax}|${extent.ymax}]"
          Row(finalFilename +: bbox +: crs.toProj4String +: metadata: _*)
        }
      }(outRowEnc)

    if (parameters.withCatalog)
      writeCatalog(pipeline, pathURI, hconf)
    else
      pipeline.foreach(_ => ())

    // The `createRelation` function here is called by
    // `org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand`, which
    // ignores the return value. It in turn returns `Seq.empty[Row]` (which is then also ignored)...
    // ¯\_(ツ)_/¯
    null
  }
}

object TilesDataSource {
  final val SHORT_NAME = "tiles"
  // writing
  final val PATH_PARAM = "path"
  final val FILENAME_COL_PARAM = "filename"
  final val CATALOG_PARAM = "catalog"
  final val METADATA_PARAM = "metadata"
  final val AS_PNG_PARAM = "png"


  protected[rasterframes]
  implicit class TilesDictAccessors(val parameters: Map[String, String]) extends AnyVal {
    def filenameColumn: Option[String] =
      parameters.get(FILENAME_COL_PARAM)

    def path: Option[URI] =
      datasource.uriParam(PATH_PARAM, parameters)

    def withCatalog: Boolean =
      parameters.get(CATALOG_PARAM).exists(_.toBoolean)

    def metadataColumns: Seq[String] =
      parameters.get(METADATA_PARAM).toSeq.flatMap(_.split(','))

    def asPNG: Boolean =
      parameters.get(AS_PNG_PARAM).exists(_.toBoolean)
  }

}
