import scala.sys.process.Process
import sbt.Keys.`package`

enablePlugins(SparkPackagePlugin, AssemblyPlugin)

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

//--------------------------------------------------------------------
// Python Build
//--------------------------------------------------------------------

lazy val pythonSource = settingKey[File]("Default Python source directory.")
pythonSource := baseDirectory.value / "python"

val Python = config("Python")

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

//Python / packageBin / crossVersion := CrossVersion.disabled

//artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
//  // Couldn't figure out how to config scope this, so having to handle for whole module
//  artifact.classifier match {
//    case Some("python") ⇒
//      val ver = version.value
//      s"${artifact.name}-python-${ver}.${artifact.extension}"
//    case _ ⇒ artifactName.value(sv, module, artifact)
//  }
//}

addArtifact(Python / packageBin / artifact, Python / packageBin)

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
  val _ = spPublishLocal.value
  val s = streams.value
  val wd = pythonSource.value
  Process("python setup.py test", wd) ! s.log match  {
    case 1 => throw new IllegalStateException("There are Python test failures.")
    case 2 => throw new IllegalStateException("Python test execution was interrupted.")
    case 3 => throw new IllegalStateException("Internal error during Python test execution.")
    case 4 => throw new IllegalStateException("PyTest usage error.")
    case 5 => throw new IllegalStateException("No Python tests found.")
    case x => if (x != 0) throw new IllegalStateException("Unknown error while running Python tests.")
  }
}

Test / test := (Test / test).dependsOn(pyTest).value

pyEgg := {
  val s = streams.value
  val wd = pythonSource.value
  Process("python setup.py bdist_egg", wd) ! s.log
}

pyExamples := {
  val _ = spPublishLocal.value
  val s = streams.value
  val wd = pythonSource.value
  Process("python setup.py examples", wd) ! s.log
}
