
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
    val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
    val `jts-core` = "org.locationtech.jts" % "jts-core" % "1.16.1"
    val `slf4j-api` = "org.slf4j" % "slf4j-api" % "1.7.25"
    val scaffeine = "com.github.blemale" %% "scaffeine" % "3.1.0"
    val `spray-json` = "io.spray" %%  "spray-json" % "1.3.4"
    val `scala-logging` = "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"
  }
  import autoImport._

  override def projectSettings = Seq(
    resolvers ++= Seq(
      "Azavea Public Builds" at "https://dl.bintray.com/azavea/geotrellis",
      "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
      "boundless-releases" at "https://repo.boundlessgeo.com/main/",
      "Open Source Geospatial Foundation Repository" at "https://download.osgeo.org/webdav/geotools/"
    ),
    /** https://github.com/lucidworks/spark-solr/issues/179
      * Thanks @pomadchin for the tip! */
    dependencyOverrides ++= {
      val deps = Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
      }
    },
    dependencyOverrides += "com.azavea.gdal" % "gdal-warp-bindings" % "33.f746890",
    // NB: Make sure to update the Spark version in pyrasterframes/python/setup.py
    rfSparkVersion := "2.4.4",
    rfGeoTrellisVersion := "3.2.0",
    rfGeoMesaVersion := "2.2.1"
  )
}
