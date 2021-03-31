import scala.sys.process.Process
import PythonBuildPlugin.autoImport.pyWhl
import com.typesafe.sbt.git.DefaultReadableGit

lazy val includeNotebooks = settingKey[Boolean]("Whether to build documentation into notebooks and include them")
includeNotebooks := true

Docker / packageName := "s22s/rasterframes-notebook"

Docker / version := version.value

dockerAliases += dockerAlias.value.withTag({
  val sha = new DefaultReadableGit(file(".")).withGit(_.headCommitSha)
  sha.map(_.take(7))
})

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

    val _ = Def.taskDyn {
      val withNB = includeNotebooks.value
      if (withNB)
        (LocalProject("pyrasterframes") / pySetup).toTask(" notebooks")
      else Def.task(0)
    }.value

    val docTarget = (LocalProject("pyrasterframes") / Python / doc / target).value
    val nbFiles = {
      if (includeNotebooks.value)
        (docTarget ** "*.ipynb").get()
      else
        (docTarget ** "*.pymd").get()
    }

    val examples = nbFiles.map(f => (f, "examples/" + f.getName))
    dockerAssets ++ Seq(py -> py.getName) ++ examples
  }
).value

// This bypasses the standard DockerPlugin DSL-based Dockerfile construction
// and just copies the separate, external one.
Docker / dockerGenerateConfig := (Docker / sourceDirectory).value / "Dockerfile"

// Save a bit of typing...
publishLocal := (Docker / publishLocal).value
publish := (Docker / publish).value

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
