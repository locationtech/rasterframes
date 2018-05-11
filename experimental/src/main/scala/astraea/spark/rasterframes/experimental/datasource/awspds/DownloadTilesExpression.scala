/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource.awspds

import java.nio.ByteBuffer

import astraea.spark.rasterframes.datasource.geotiff.GeoTiffInfoSupport
import astraea.spark.rasterframes.encoders.StandardEncoders
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.SpatialKey
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, UnaryExpression}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/**
 * Catalyst generator to convert a geotiff download URL into a series of rows containing the internal
 * tiles and associated extents.
 *
 * @since 5/4/18
 */
case class DownloadTilesExpression(override val child: Expression, colPrefix: String) extends UnaryExpression
  with Generator with CodegenFallback with GeoTiffInfoSupport with StandardEncoders with DownloadSupport with LazyLogging {

  override def checkInputDataTypes(): TypeCheckResult = {
    if(child.dataType == StringType) TypeCheckSuccess
    else TypeCheckFailure(
      s"Expected '${StringType.typeName}' but received '${child.dataType.simpleString}'"
    )
  }

  private def tlmEncoder = tileLayerMetadataEncoder[SpatialKey]

  override def elementSchema: StructType = StructType(Seq(
    StructField(colPrefix + "_metadata", tlmEncoder.schema, false),
    StructField(colPrefix + "_spatial_key", spatialKeyEncoder.schema, false),
    StructField(colPrefix + "_extent", extentEncoder.schema, false),
    StructField(colPrefix + "_tile", new TileUDT, false)
  ))

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val urlString = child.eval(input).asInstanceOf[UTF8String]
      val bytes = ByteBuffer.wrap(downloadBytes(urlString.toString))
      val (info, layerMetadata) = extractGeoTiffLayout(bytes)

      val tileLayout = info.segmentLayout.tileLayout

      val tile = GeoTiffReader.geoTiffSinglebandTile(info)
      tile
        .split(tileLayout)
        .zipWithIndex
        .map { case (tile, index) =>
          val layoutCol = index % tileLayout.layoutCols
          val layoutRow = index / tileLayout.layoutCols
          val sk = SpatialKey(layoutCol, layoutRow)
          val extent = layerMetadata.mapTransform(sk)
          val t = TileUDT.serialize(tile)
          val e = extentEncoder.toRow(extent)
          val s = spatialKeyEncoder.toRow(sk)
          val tlm = tlmEncoder.toRow(layerMetadata)
          InternalRow(tlm, s, e, t)
        }
    }
    catch {
      case NonFatal(ex) ⇒ logger.error("Error fetching data", ex)
        Traversable.empty
    }
  }
}
