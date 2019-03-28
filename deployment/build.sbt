import sbt.{IO, _}

import scala.sys.process.Process

moduleName := "rasterframes-deployment"

val Docker = config("docker")
val Python = config("python")


lazy val rfDockerImageName = settingKey[String]("Name to tag Docker image with.")
rfDockerImageName := "s22s/rasterframes-notebooks"

lazy val rfDocker = taskKey[Unit]("Build Jupyter Notebook Docker image with RasterFrames support.")
rfDocker := (Docker / packageBin).value

lazy val runRFNotebook = taskKey[String]("Run RasterFrames Jupyter Notebook image")
runRFNotebook := {
  val imageName = rfDockerImageName.value
  val _ = rfDocker.value
  Process(s"docker run -p 8888:8888 -p 4040:4040 $imageName").run()
  imageName
}

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
  val imageName = rfDockerImageName.value
  Process("docker-compose build", staging).!
  Process(s"docker tag $imageName:latest $imageName:$ver", staging).!
  staging
}
