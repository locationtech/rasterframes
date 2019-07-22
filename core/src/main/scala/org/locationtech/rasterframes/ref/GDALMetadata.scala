/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Azavea, Inc.
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

package org.locationtech.rasterframes.ref
import com.azavea.gdal.GDALWarp
import geotrellis.contrib.vlm.gdal.{acceptableDatasets, numberOfAttempts}
import geotrellis.raster.io.geotiff.Tags
import org.locationtech.rasterframes.ref.GDALMetadata.BASE_METADATA_DOMAIN

/**
  * This code copied from here:
  * https://github.com/pomadchin/geotrellis-contrib/blob/770f0a49918ad7b55ec791c0a76504f2aa169d4e/gdal/src/main/scala/geotrellis/contrib/vlm/gdal/GDALDataset.scala#L27-L58
  *
  * Remove once GeoTrellis 3.0 is released and we upgrade to the latest `geotrellis-contrib`.
  *
  * @param token Dataset handle.
  */
case class GDALMetadata(token: Long) extends AnyVal {

  def getMetadataDomainList(dataset: Int, band: Int): List[String] = {
    val arr = Array.ofDim[Byte](10, 1 << 10)
    GDALWarp.get_metadata_domain_list(token, dataset, numberOfAttempts, band, arr)
    arr.map(new String(_, "UTF-8").trim).filter(_.nonEmpty).toList
  }

  def getMetadata(dataset: Int, domain: String, band: Int): Map[String, String] = {
    val arr = Array.ofDim[Byte](10, 1 << 10)
    GDALWarp.get_metadata(token, dataset, numberOfAttempts, band, domain, arr)
    arr
      .map(new String(_, "UTF-8").trim)
      .filter(_.nonEmpty)
      .flatMap { str =>
        val arr = str.split("=")
        if(arr.length == 2) {
          val Array(key, value) = str.split("=")
          Some(key -> value)
        } else Some("" -> str)
      }.toMap
  }

  def getMetadataItem(dataset: Int, key: String, domain: String, band: Int): String = {
    val arr = Array.ofDim[Byte](1 << 10)
    GDALWarp.get_metadata_item(token, dataset, numberOfAttempts, band, key, domain, arr)
    new String(arr, "UTF-8").trim
  }

  def bandCount: Int = bandCount(GDALWarp.SOURCE)

  def bandCount(dataset: Int): Int = {
    require(acceptableDatasets contains dataset)
    val count = Array.ofDim[Int](1)
    if (GDALWarp.get_band_count(token, dataset, numberOfAttempts, count) <= 0)
      throw new Exception("get_band_count")
    count(0)
  }

  def toTags: Tags = {
    val mdSets = for {
      b <- 0 to bandCount(GDALWarp.SOURCE)
    } yield {
      val sub = for {
        domain <- BASE_METADATA_DOMAIN +: getMetadataDomainList(GDALWarp.SOURCE, b)
      } yield getMetadata(GDALWarp.SOURCE, domain, b)
      sub.reduceLeft(_ ++ _)
    }

    Tags(mdSets.head, mdSets.tail.toList)
  }

  override def toString: String = {
    val lines = for {
      b <- 0 to bandCount(GDALWarp.SOURCE) // Band 0 is for global metadata.
      domain <- BASE_METADATA_DOMAIN +: getMetadataDomainList(GDALWarp.SOURCE, b)
      pair <- getMetadata(GDALWarp.SOURCE, domain, b)
    } yield s"$domain[$b]::${pair._1}=${pair._2}"

    lines.mkString("\n")
  }
}

object GDALMetadata {
  final val BASE_METADATA_DOMAIN = ""
}
