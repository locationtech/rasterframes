import sbt.{IO, _}

import scala.sys.process.Process

Docker / packageName := "rasterframes-notebook"

Docker / version := version.value

Docker / maintainer := organization.value

Docker / sourceDirectory := baseDirectory.value / "src"/ "main" / "docker"

Docker / target := target.value / "docker"

dockerUpdateLatest := true

Docker / mappings := {
  val dockerSrc = (Docker / sourceDirectory).value
  val dockerAssets = (dockerSrc ** "*") pair Path.relativeTo(dockerSrc)

  val jar = assembly.value

  val py = (LocalProject("pyrasterframes") / Python / packageBin).value

  dockerAssets ++ Seq(jar -> jar.getName, py -> py.getName)
}

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
