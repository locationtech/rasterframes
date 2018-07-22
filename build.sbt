addCommandAlias("makeSite", "docs/makeSite")
addCommandAlias("console", "datasource/console")

lazy val root = project
  .in(file("."))
  .withId("RasterFrames")
  .aggregate(core, datasource, pyrasterframes, experimental)
  .settings(publish / skip := true)
  .settings(releaseSettings)

lazy val deployment = project
  .dependsOn(root)
  .disablePlugins(SparkPackagePlugin)

lazy val core = project
  .disablePlugins(SparkPackagePlugin)

lazy val pyrasterframes = project
  .dependsOn(core, datasource, experimental)
  .settings(assemblySettings)

lazy val datasource = project
  .dependsOn(core % "test->test;compile->compile")
  .disablePlugins(SparkPackagePlugin)

lazy val experimental = project
  .dependsOn(core % "test->test;compile->compile")
  .dependsOn(datasource % "test->test;compile->compile")
  .disablePlugins(SparkPackagePlugin)

lazy val docs = project
  .dependsOn(core, datasource)
  .disablePlugins(SparkPackagePlugin)

lazy val bench = project
  .dependsOn(core)
  .disablePlugins(SparkPackagePlugin)
  .settings(publish / skip := true)

