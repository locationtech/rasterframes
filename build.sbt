/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2019 Astraea, Inc.
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

// Leave me an my custom keys alone!
Global / lintUnusedKeysOnLoad := false

addCommandAlias("makeSite", "docs/makeSite")
addCommandAlias("previewSite", "docs/previewSite")
addCommandAlias("ghpagesPushSite", "docs/ghpagesPushSite")
addCommandAlias("console", "datasource/console")

// Prefer our own IntegrationTest config definition, which inherits from Test.
lazy val IntegrationTest = config("it") extend Test

lazy val root = project
  .in(file("."))
  .withId("RasterFrames")
  .aggregate(core, datasource, pyrasterframes)
  .enablePlugins(RFReleasePlugin)
  .settings(
    publish / skip := true,
    clean := clean.dependsOn(`rf-notebook`/clean, docs/clean).value
  )

lazy val `rf-notebook` = project
  .dependsOn(pyrasterframes)
  .enablePlugins(RFAssemblyPlugin, DockerPlugin)
  .settings(publish / skip := true)

lazy val core = project
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(Defaults.itSettings)
  .settings(
    moduleName := "rasterframes",
    libraryDependencies ++= Seq(
      `slf4j-api`,
      shapeless,
      circe("core").value,
      circe("generic").value,
      circe("parser").value,
      circe("generic-extras").value,
      frameless excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      `jts-core`,
      `spray-json`,
      geomesa("z3").value,
      geomesa("spark-jts").value,
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided,
      // TODO: scala-uri brings an outdated simulacrum dep
      // Fix it in GT
      geotrellis("spark").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      geotrellis("raster").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      geotrellis("s3").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      geotrellis("spark-testkit").value % Test excludeAll (
        ExclusionRule(organization = "org.scalastic"),
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "com.github.mpilquist")
      ),
      scaffeine,
      scalatest,
      `scala-logging`
    ),
    libraryDependencies ++= {
      val gv = rfGeoTrellisVersion.value
      if (gv.startsWith("3")) Seq[ModuleID](
        geotrellis("gdal").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
        geotrellis("s3-spark").value excludeAll ExclusionRule(organization = "com.github.mpilquist")
      )
      else Seq.empty[ModuleID]
    },
    buildInfoKeys ++= Seq[BuildInfoKey](
      version, scalaVersion, rfGeoTrellisVersion, rfGeoMesaVersion, rfSparkVersion
    ),
    buildInfoPackage := "org.locationtech.rasterframes",
    buildInfoObject := "RFBuildInfo",
    buildInfoOptions := Seq(
      BuildInfoOption.ToMap,
      BuildInfoOption.ToJson
    )
  )

lazy val pyrasterframes = project
  .dependsOn(core, datasource, experimental)
  .enablePlugins(RFAssemblyPlugin, PythonBuildPlugin)
  .settings(
    libraryDependencies ++= Seq(
      geotrellis("s3").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided
    )
  )

lazy val datasource = project
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(core % "test->test;compile->compile")
  .settings(
    moduleName := "rasterframes-datasource",
    libraryDependencies ++= Seq(
      compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
      compilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
      sttpCatsCe2,
      stac4s,
      framelessRefined excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      geotrellis("s3").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided,
      `better-files`
    ),
    Compile / console / scalacOptions ~= { _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports")) },
    Test / console / scalacOptions ~= { _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports")) },
    console / initialCommands := (console / initialCommands).value +
      """
        |import org.locationtech.rasterframes.datasource.geotrellis._
        |import org.locationtech.rasterframes.datasource.geotiff._
        |""".stripMargin,
    IntegrationTest / fork := true,
    IntegrationTest / javaOptions := Seq("-Xmx3g")
  )

lazy val experimental = project
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(core % "test->test;it->test;compile->compile")
  .dependsOn(datasource % "test->test;it->test;compile->compile")
  .settings(
    moduleName := "rasterframes-experimental",
    libraryDependencies ++= Seq(
      geotrellis("s3").value excludeAll ExclusionRule(organization = "com.github.mpilquist"),
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided
    ),
    IntegrationTest / fork := true,
    IntegrationTest / javaOptions := (datasource / IntegrationTest / javaOptions).value
  )

lazy val docs = project
  .dependsOn(core, datasource, pyrasterframes)
  .enablePlugins(SiteScaladocPlugin, ParadoxPlugin, ParadoxMaterialThemePlugin, GhpagesPlugin, ScalaUnidocPlugin)
  .settings(
    apiURL := Some(url("https://rasterframes.io/latest/api")),
    autoAPIMappings := true,
    ghpagesNoJekyll := true,
    ScalaUnidoc / siteSubdirName := "latest/api",
    paradox / siteSubdirName := ".",
    paradoxProperties ++= Map(
      "version" -> version.value,
      "scaladoc.org.apache.spark.sql.rf" -> "https://rasterframes.io/latest",
      "github.base_url" -> ""
    ),
    paradoxNavigationExpandDepth := Some(3),
    Compile / paradoxMaterialTheme ~= { _
      .withRepository(uri("https://github.com/locationtech/rasterframes"))
      .withCustomStylesheet("assets/custom.css")
      .withCopyright("""&copy; 2017-2021 <a href="https://astraea.earth">Astraea</a>, Inc. All rights reserved.""")
      .withLogo("assets/images/RF-R.svg")
      .withFavicon("assets/images/RasterFrames_32x32.ico")
      .withColor("blue-grey", "light-blue")
      .withGoogleAnalytics("UA-106630615-1")
    },
    makeSite := makeSite
      .dependsOn(Compile / unidoc)
      .dependsOn((Compile / paradox)
        .dependsOn(pyrasterframes / doc)
      ).value,
    Compile / paradox / sourceDirectories += (pyrasterframes / Python / doc / target).value,
  )
  .settings(
    addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, ScalaUnidoc / siteSubdirName)
  )
  .settings(
    addMappingsToSiteDir(Compile / paradox / mappings, paradox / siteSubdirName)
  )

lazy val bench = project
  .dependsOn(core % "compile->test")
  .settings(publish / skip := true)
