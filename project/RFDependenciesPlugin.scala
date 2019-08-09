
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

import sbt.Keys._
import sbt._

object RFDependenciesPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  object autoImport {
    val rfSparkVersion = settingKey[String]("Apache Spark version")
    val rfGeoTrellisVersion = settingKey[String]("GeoTrellis version")
    val rfGeoMesaVersion = settingKey[String]("GeoMesa version")

    def geotrellis(module: String) = Def.setting {
      "org.locationtech.geotrellis" %% s"geotrellis-$module" % rfGeoTrellisVersion.value
    }
    def spark(module: String) = Def.setting {
      "org.apache.spark" %% s"spark-$module" % rfSparkVersion.value
    }
    def geomesa(module: String) = Def.setting {
      "org.locationtech.geomesa" %% s"geomesa-$module" % rfGeoMesaVersion.value
    }

    val scalatest = "org.scalatest" %% "scalatest" % "3.0.3" % Test
    val shapeless = "com.chuusai" %% "shapeless" % "2.3.2"
    val `jts-core` = "org.locationtech.jts" % "jts-core" % "1.16.0"
    val `geotrellis-contrib-vlm` = "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "2.11.0"
    val `geotrellis-contrib-gdal` = "com.azavea.geotrellis" %% "geotrellis-contrib-gdal" % "2.11.0"

    val scaffeine = "com.github.blemale" %% "scaffeine" % "2.6.0"
  }
  import autoImport._

  override def projectSettings = Seq(
    resolvers ++= Seq(
      "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
      "Azavea Public Builds" at "https://dl.bintray.com/azavea/geotrellis",
      "boundless-releases" at "https://repo.boundlessgeo.com/main/",
      "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools/"
    ),

    // NB: Make sure to update the Spark version in pyrasterframes/python/setup.py
    rfSparkVersion := "2.3.3",
    rfGeoTrellisVersion := "2.2.0",
    rfGeoMesaVersion := "2.2.1",
    dependencyOverrides += "com.azavea.gdal" % "gdal-warp-bindings" % "33.58d4965"
  )
}
