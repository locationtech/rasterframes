import scala.sys.process.Process
import PythonBuildPlugin.autoImport.pyWhl

lazy val includeNotebooks = settingKey[Boolean]("Whether to build documentation into notebooks and include them")
includeNotebooks := true

Docker / packageName := "s22s/rasterframes-notebook"

Docker / version := version.value

Docker / maintainer := organization.value

Docker / sourceDirectory := baseDirectory.value / "src"/ "main" / "docker"

Docker / target := target.value / "docker"

dockerUpdateLatest := true

Docker / mappings := Def.sequential(
  LocalProject("pyrasterframes") / pyWhl,
  Def.task  {
    val dockerSrc = (Docker / sourceDirectory).value
    val dockerAssets = (dockerSrc ** "*") pair Path.relativeTo(dockerSrc)

    val py = (LocalProject("pyrasterframes") / pyWhl).value

    Def.taskDyn {
      val withNB = includeNotebooks.value
      if (withNB)
        (LocalProject("pyrasterframes") / pySetup).toTask(" notebooks")
      else Def.task(0)
    }.value

    val nbFiles = ((LocalProject("pyrasterframes") / Python / doc / target).value ** "*.ipynb").get()

    val examples = nbFiles.map(f => (f, "examples/" + f.getName))
    dockerAssets ++ Seq(py -> py.getName) ++ examples
  }
).value

// This bypasses the standard DockerPlugin DSL-based Dockerfile construction
// and just copies the separate, external one.
Docker / dockerGenerateConfig := (Docker / sourceDirectory).value / "Dockerfile"

// Save a bit of typing...
publishLocal := (Docker / publishLocal).value

// -----== Conveniences ==-----

lazy val startRFNotebook = Def.taskKey[Unit]("Build and run a RasterFrames Notebook Docker container")
lazy val quickStartRFNotebook = Def.taskKey[Unit]("Run RasterFrames Notebook Docker container without building it first")
lazy val stopRFNotebook = Def.taskKey[Unit]("Stop a running RasterFrames Notebook instance")

startRFNotebook := {
  val _ = (Docker / publishLocal).value
  val staging = (Docker / stagingDirectory).value
  Process("docker-compose up --force-recreate -d", staging).!
}

quickStartRFNotebook := {
  val staging = (Docker / stagingDirectory).value
  Process("docker-compose up --force-recreate -d", staging).!
}

stopRFNotebook := {
  val staging = (Docker / stagingDirectory).value
  Process("docker-compose down", staging).!
}
