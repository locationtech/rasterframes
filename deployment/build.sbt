import sbt.{IO, _}

import scala.sys.process.Process

moduleName := "rasterframes-deployment"

lazy val rfNotebookContainer = taskKey[Unit]("Build a Docker container that supports RasterFrames notebooks.")
rfNotebookContainer := (Docker / packageBin).value

val Docker = config("docker")
val Python = config("python")

Docker / resourceDirectory := baseDirectory.value / "docker"/ "jupyter"

Docker / target := target.value / "docker"

Docker / mappings := {
  val rezDir = (Docker / resourceDirectory).value
  val files = (rezDir ** "*") pair Path.relativeTo(rezDir)

  val jar = (assembly in LocalProject("pyrasterframes")).value
  val py = (packageBin in (LocalProject("pyrasterframes"), Python)).value

  files ++ Seq(jar -> jar.getName, py -> py.getName)
}

def rfFiles = Def.task {
  val destDir = (Docker / target).value
  val filePairs = (Docker / mappings).value
  IO.copy(filePairs.map { case (src, dst) ⇒ (src, destDir / dst) })
}

Docker / packageBin := {
  val _ = rfFiles.value
  val logger = streams.value.log
  val staging = (Docker / target).value
  val ver = (version in LocalRootProject).value

  logger.info(s"Running docker build in $staging")
  val imageName = "s22s/rasterframes-notebooks"
  Process("docker-compose build", staging).!
  Process(s"docker tag $imageName:latest $imageName:$ver", staging).!
  staging
}
