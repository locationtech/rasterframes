
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
      "org.locationtech.geotrellis" %% s"geotrellis-$module" % rfGeoTrellisVersion.value excludeAll("org.scala-lang.modules", "scala-xml")
    }
    def spark(module: String) = Def.setting {
      "org.apache.spark" %% s"spark-$module" % rfSparkVersion.value
    }
    def geomesa(module: String) = Def.setting {
      "org.locationtech.geomesa" %% s"geomesa-$module" % rfGeoMesaVersion.value
    }
    def circe(module: String) = Def.setting {
      module match {
        case "json-schema" => "io.circe" %% s"circe-$module" % "0.2.0"
        case _             => "io.circe" %% s"circe-$module" % "0.14.1"
      }
    }
    val scalatest = "org.scalatest" %% "scalatest" % "3.2.5" % Test
    val shapeless = "com.chuusai" %% "shapeless" % "2.3.9"
    val `jts-core` = "org.locationtech.jts" % "jts-core" % "1.18.1"
    val `slf4j-api` = "org.slf4j" % "slf4j-api" % "1.7.36"
    val scaffeine = "com.github.blemale" %% "scaffeine" % "4.1.0"
    val `spray-json` = "io.spray" %%  "spray-json" % "1.3.6"
    val `scala-logging` = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
    val stac4s = "com.azavea.stac4s" %% "client" % "0.8.1"
    val sttpCatsCe2 = "com.softwaremill.sttp.client3" %% "async-http-client-backend-cats-ce2" % "3.8.12"
    val frameless = "org.typelevel" %% "frameless-dataset" % "0.13.0"
    val framelessRefined = "org.typelevel" %% "frameless-refined" % "0.13.0"
    val `better-files` = "com.github.pathikrit" %% "better-files" % "3.9.1" % Test
    val sparktestingbase = "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0" % Test

  }
  import autoImport._

  override def projectSettings = Seq(
    resolvers ++= Seq(
      "eclipse-releases" at "https://repo.locationtech.org/content/groups/releases",
      "eclipse-snapshots" at "https://repo.eclipse.org/content/groups/snapshots",
      "boundless-releases" at "https://repo.boundlessgeo.com/main/",
      "Open Source Geospatial Foundation Repository" at "https://download.osgeo.org/webdav/geotools/",
      "oss-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "jitpack" at "https://jitpack.io"
    ),
    // NB: Make sure to update the Spark version in pyrasterframes/python/setup.py
    rfSparkVersion := "3.3.1",
    rfGeoTrellisVersion := "3.6.3",
    rfGeoMesaVersion := "3.5.1"
  )
}
