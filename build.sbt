
addCommandAlias("makeSite", "docs/makeSite")
addCommandAlias("previewSite", "docs/previewSite")
addCommandAlias("console", "datasource/console")

lazy val root = project
  .in(file("."))
  .withId("RasterFrames")
  .aggregate(core, datasource, pyrasterframes, experimental)
  .enablePlugins(RFReleasePlugin)
  .settings(publish / skip := true)

lazy val `rf-notebook` = project
  .dependsOn(pyrasterframes)
  .enablePlugins(RFAssemblyPlugin, DockerPlugin)

lazy val IntegrationTest = config("it") extend Test

lazy val core = project
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(Defaults.itSettings)
  .settings(
    moduleName := "rasterframes",
    libraryDependencies ++= Seq(
      shapeless,
      `jts-core`,
      geomesa("z3").value,
      geomesa("spark-jts").value,
      `geotrellis-contrib-vlm`,
      `geotrellis-contrib-gdal`,
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided,
      geotrellis("spark").value,
      geotrellis("raster").value,
      geotrellis("s3").value,
      geotrellis("spark-testkit").value % Test excludeAll (
        ExclusionRule(organization = "org.scalastic"),
        ExclusionRule(organization = "org.scalatest")
      ),
      scaffeine,
      scalatest
    ),
    buildInfoKeys ++= Seq[BuildInfoKey](
      moduleName, version, scalaVersion, sbtVersion, rfGeoTrellisVersion, rfGeoMesaVersion, rfSparkVersion
    ),
    buildInfoPackage := "org.locationtech.rasterframes",
    buildInfoObject := "RFBuildInfo",
    buildInfoOptions := Seq(
      BuildInfoOption.ToMap,
      BuildInfoOption.BuildTime,
      BuildInfoOption.ToJson
    )
  )

lazy val pyrasterframes = project
  .dependsOn(core, datasource, experimental)
  .enablePlugins(RFAssemblyPlugin, PythonBuildPlugin)
  .settings(
    libraryDependencies ++= Seq(
      geotrellis("s3").value,
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided
    )
  )

lazy val datasource = project
  .dependsOn(core % "test->test;compile->compile")
  .settings(
    moduleName := "rasterframes-datasource",
    libraryDependencies ++= Seq(
      geotrellis("s3").value,
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided
    ),
    initialCommands in console := (initialCommands in console).value +
      """
        |import org.locationtech.rasterframes.datasource.geotrellis._
        |import org.locationtech.rasterframes.datasource.geotiff._
        |""".stripMargin
  )

lazy val experimental = project
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(core % "test->test;it->test;compile->compile")
  .dependsOn(datasource % "test->test;it->test;compile->compile")
  .settings(
    moduleName := "rasterframes-experimental",
    libraryDependencies ++= Seq(
      geotrellis("s3").value,
      spark("core").value % Provided,
      spark("mllib").value % Provided,
      spark("sql").value % Provided
    ),
    fork in IntegrationTest := true,
    javaOptions in IntegrationTest := Seq("-Xmx2G"),
    parallelExecution in IntegrationTest := false
  )

lazy val docs = project
  .dependsOn(core, datasource, pyrasterframes)
  .enablePlugins(SiteScaladocPlugin, ParadoxPlugin, GhpagesPlugin, ScalaUnidocPlugin)
  .settings(
    apiURL := Some(url("http://rasterframes.io/latest/api")),
    autoAPIMappings := true,
    ghpagesNoJekyll := true,
    ScalaUnidoc / siteSubdirName := "latest/api",
    paradox / siteSubdirName := ".",
    paradoxProperties ++= Map(
      "github.base_url" -> "https://github.com/locationtech/rasterframes",
      "version" -> version.value,
      "scaladoc.org.apache.spark.sql.rf" -> "http://rasterframes.io/latest"
    ),
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    makeSite := makeSite.dependsOn(Compile / unidoc).dependsOn(Compile / paradox).value,
    Compile / paradox / sourceDirectories += (pyrasterframes / Python / doc / target).value,
    Compile / paradox := (Compile / paradox).dependsOn(pyrasterframes / doc).value
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

