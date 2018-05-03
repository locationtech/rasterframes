import scala.sys.process.Process
import sbt.Keys.`package`

enablePlugins(SparkPackagePlugin, AssemblyPlugin)

lazy val pyZip = taskKey[File]("Create a minimal pyrasterframes zip distribution.")

val pysparkCmd = taskKey[Unit]("Builds pyspark package and emits command string for running pyspark with package")

lazy val pyTest = taskKey[Unit]("Run pyrasterframes tests.")

lazy val pyExamples = taskKey[Unit]("Run pyrasterframes examples.")

lazy val pyEgg = taskKey[Unit]("Creates a Python .egg file")

lazy val spJarFile = Def.taskDyn {
  if (spShade.value) {
    Def.task((assembly in spPackage).value)
  } else {
    Def.task(spPackage.value)
  }
}

spName := "io.astraea/pyrasterframes"
sparkVersion := rfSparkVersion.value
sparkComponents ++= Seq("sql", "mllib")
spAppendScalaVersion := false
spIncludeMaven := false
spIgnoreProvided := true
spShade := true
spPackage := {
  val dist = spDist.value
  val extracted = IO.unzip(dist, (target in spPackage).value, GlobFilter("*.jar"))
  if(extracted.size != 1) sys.error("Didn't expect to find multiple .jar files in distribution.")
  extracted.head
}
spShortDescription := description.value
spHomepage := homepage.value.get.toString
spDescription := """
                   |RasterFrames brings the power of Spark DataFrames to geospatial raster data,
                   |empowered by the map algebra and tile layer operations of GeoTrellis.
                   |
                   |The underlying purpose of RasterFrames is to allow data scientists and software
                   |developers to process and analyze geospatial-temporal raster data with the
                   |same flexibility and ease as any other Spark Catalyst data type. At its core
                   |is a user-defined type (UDT) called TileUDT, which encodes a GeoTrellis Tile
                   |in a form the Spark Catalyst engine can process. Furthermore, we extend the
                   |definition of a DataFrame to encompass some additional invariants, allowing
                   |for geospatial operations within and between RasterFrames to occur, while
                   |still maintaining necessary geo-referencing constructs.
                 """.stripMargin

test in assembly := {}

spPublishLocal := {
  // This unfortunate override is necessary because
  // the ivy resolver in pyspark defaults to the cache more
  // frequently than we'd like.
  val id = (projectID in spPublishLocal).value
  val home = ivyPaths.value.ivyHome
    .getOrElse(io.Path.userHome / ".ivy2")
  val cacheDir = home / "cache" / id.organization / id.name
  IO.delete(cacheDir)
  spPublishLocal.value
}

pyZip := {
  val jar = (`package` in Compile).value
  val license = baseDirectory.value / "python" / "LICENSE.md"
  val pyDir = baseDirectory.value / "python" / "pyrasterframes"
  val files = (IO.listFiles(pyDir, GlobFilter("*.py") | GlobFilter("*.rst")) ++ Seq(jar, license))
    .map(f => (f, "pyrasterframes/" + f.getName))
  val zipFile = target.value / "python-dist" / "pyrasterframes.zip"
  IO.zip(files, zipFile)

  zipFile
}

pysparkCmd := {
  val _ = spPublishLocal.value
  val id = (projectID in spPublishLocal).value
  val args = "pyspark" ::  "--packages" :: s"${id.organization}:${id.name}:${id.revision}" :: Nil
  streams.value.log.info("PySpark Command:\n" + args.mkString(" "))
  // --conf spark.jars.ivy=(ivyPaths in pysparkCmd).value....
}

ivyPaths in pysparkCmd := ivyPaths.value.withIvyHome(target.value / "ivy")

//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

pyTest := {
  val _ = spPublishLocal.value
  val s = streams.value
  val wd = baseDirectory.value / "python"
  Process("python setup.py test", wd) ! s.log
}

Test / test := (Test / test).dependsOn(pyTest).value

pyEgg := {
  val s = streams.value
  val wd = baseDirectory.value / "python"
  Process("python setup.py bdist_egg", wd) ! s.log
}

pyExamples := {
  val _ = spPublishLocal.value
  val s = streams.value
  val wd = baseDirectory.value / "python"
  Process("python setup.py examples", wd) ! s.log
}

