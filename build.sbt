addCommandAlias("makeSite", "docs/makeSite")
addCommandAlias("console", "datasource/console")

lazy val root = project
  .in(file("."))
  .withId("RasterFrames")
  .aggregate(core, datasource, pyrasterframes, experimental)
  .enablePlugins(RFReleasePlugin)
  .settings(publish / skip := true)

lazy val deployment = project
  .dependsOn(root)
  .disablePlugins(SparkPackagePlugin)

lazy val IntegrationTest = config("it") extend Test

lazy val core = project
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(Defaults.itSettings)
  .disablePlugins(SparkPackagePlugin)
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
      scalatest
    ),
    buildInfoKeys ++= Seq[BuildInfoKey](
      name, version, scalaVersion, sbtVersion, rfGeoTrellisVersion, rfGeoMesaVersion, rfSparkVersion
    ),
    buildInfoPackage := "org.locationtech.rasterframes",
    buildInfoObject := "RFBuildInfo",
    buildInfoOptions := Seq(
      BuildInfoOption.ToMap,
      BuildInfoOption.BuildTime
    )
  )

lazy val pyrasterframes = project
  .dependsOn(core, datasource, experimental)
  .enablePlugins(RFAssemblyPlugin)

lazy val datasource = project
  .dependsOn(core % "test->test;compile->compile")
  .disablePlugins(SparkPackagePlugin)
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
  .disablePlugins(SparkPackagePlugin)
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
  .dependsOn(core, datasource)
  .disablePlugins(SparkPackagePlugin)

lazy val bench = project
  .dependsOn(core % "compile->test")
  .disablePlugins(SparkPackagePlugin)
  .settings(publish / skip := true)

