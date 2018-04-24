addCommandAlias("makeSite", "docs/makeSite")
addCommandAlias("console", "datasource/console")

lazy val root = project
  .in(file("."))
  .withId("RasterFrames")
  .aggregate(core, datasource, pyrasterframes, experimental)
  .settings(publishArtifact := false)
  .settings(releaseSettings)

lazy val core = project
  .disablePlugins(SparkPackagePlugin)

lazy val pyrasterframes = project
  .dependsOn(core, datasource)
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


