import scala.sys.process.Process
import sbt.Keys.`package`
import sbt.Tests.Summary

lazy val assemblyHasGeoTrellis = settingKey[Boolean]("If false, GeoTrellis libraries are excluded from assembly.")
assemblyHasGeoTrellis := true

assembly / assemblyExcludedJars ++= {
  val cp = (fullClasspath in assembly).value
  val gt = assemblyHasGeoTrellis.value
  if(gt) Seq.empty
  else cp.filter(_.data.getName.contains("geotrellis"))
}

assembly / test := {}

//--------------------------------------------------------------------
// Spark Packages Plugin
//--------------------------------------------------------------------

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

//--------------------------------------------------------------------
// Python Build
//--------------------------------------------------------------------

lazy val pythonSource = settingKey[File]("Default Python source directory.")
pythonSource := baseDirectory.value / "python"

def sparkFiles(dir: File):Seq[File] = Seq("metastore_db",
                                          "spark-warehouse",
                                          "derby.log").map(f => dir / f)

cleanFiles ++=
  sparkFiles(baseDirectory.value) ++
  sparkFiles(pythonSource.value) ++
  Seq(
    ".eggs",
    ".pytest_cache",
    "build",
    "dist",
    "pyrasterframes.egg-info"
  ).map(f => pythonSource.value / f)

val Python = config("python")

Python / target := target.value / "python-dist"

lazy val pyZip = taskKey[File]("Build pyrasterframes zip.")

// Alias
pyZip := (Python / packageBin).value

Python / packageBin / artifact := {
  val java = (Compile / packageBin / artifact).value
  java.withType("zip").withClassifier(Some("python")).withExtension("zip")
}

Python / packageBin / artifactPath := {
  val dir = (Python / target).value
  val art = (Python / packageBin / artifact).value
  val ver = version.value
  dir / s"${art.name}-python-$ver.zip"
}

addArtifact(Python / packageBin / artifact, Python / packageBin)

val pysparkCmd = taskKey[Unit]("Builds pyspark package and emits command string for running pyspark with package")

lazy val pyTest = taskKey[Int]("Run pyrasterframes tests. Return result code.")

lazy val pyExamples = taskKey[Unit]("Run pyrasterframes examples.")

lazy val pyWheel = taskKey[Unit]("Creates a Python .whl file")

lazy val spJarFile = Def.taskDyn {
  if (spShade.value) {
    Def.task((assembly in spPackage).value)
  } else {
    Def.task(spPackage.value)
  }
}

def gatherFromDir(root: sbt.File, dirName: String) = {
  val pyDir = root / dirName
  IO.listFiles(pyDir, GlobFilter("*.py") | GlobFilter("*.rst"))
    .map(f => (f, s"$dirName/${f.getName}"))
}

Python / packageBin := {
  val jar = (`package` in Compile).value
  val root = pythonSource.value
  val license = root / "LICENSE.md"
  val files = gatherFromDir(root, "pyrasterframes") ++
    gatherFromDir(root, "geomesa_pyspark") ++
    Seq(jar, license).map(f => (f, "pyrasterframes/" + f.getName))
  val zipFile = (Python / packageBin / artifactPath).value
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

pyTest := {
  val _ = assembly.value
  val s = streams.value
  s.log.info("Running python tests...")
  val wd = pythonSource.value
  Process("python setup.py test", wd).!
}

Test / executeTests := {
  val standard = (Test / executeTests).value
  standard.overall match {
    case TestResult.Passed ⇒
      val resultCode = pyTest.value
      val msg = resultCode match {
        case 1 ⇒ "There are Python test failures."
        case 2 ⇒ "Python test execution was interrupted."
        case 3 ⇒ "Internal error during Python test execution."
        case 4 ⇒ "PyTest usage error."
        case 5 ⇒ "No Python tests found."
        case x if (x != 0) ⇒ "Unknown error while running Python tests."
        case _ ⇒ "PyRasterFrames tests successfully completed."
      }
      val pySummary = Summary("pyrasterframes", msg)
      val summaries = standard.summaries ++ Iterable(pySummary)
      // Would be cool to derive this from the python output...
      val result = if(resultCode == 0) {
        new SuiteResult(
          TestResult.Passed,
          passedCount = 1,
          failureCount = 0,
          errorCount = 0,
          skippedCount = 0,
          ignoredCount = 0,
          canceledCount = 0,
          pendingCount = 0
        )
      }
      else {
        new SuiteResult(
          TestResult.Failed,
          passedCount = 0,
          failureCount = 1,
          errorCount = 0,
          skippedCount = 0,
          ignoredCount = 0,
          canceledCount = 0,
          pendingCount = 0
        )
      }
      standard.copy(overall = result.result, summaries = summaries, events = standard.events + ("PyRasterFramesTests" -> result))
    case _ ⇒
      val pySummary = Summary("pyrasterframes", "tests skipped due to scalatest failures")
      standard.copy(summaries = standard.summaries ++ Iterable(pySummary))
  }
}

pyWheel := {
  val s = streams.value
  val wd = pythonSource.value
  Process("python setup.py bdist_wheel", wd) ! s.log
  val whl = IO.listFiles(pythonSource.value / "dist", GlobFilter("*.whl"))(0)
  IO.move(whl, (Python / target).value / whl.getName)
}

pyExamples := {
  val _ = spPublishLocal.value
  val s = streams.value
  val wd = pythonSource.value
  Process("python setup.py examples", wd) ! s.log
}
