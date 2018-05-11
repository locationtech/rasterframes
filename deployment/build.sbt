
import sbt.{IO, _}

import scala.sys.process.Process

moduleName := "rasterframes-deployment"

lazy val rfNotebookContainer = taskKey[File]("Build a Docker container that supports RasterFrames notebooks.")

rfNotebookContainer := {
  val logger = streams.value.log
  val wd = baseDirectory.value / "docker"/ "jupyter"

  val ver = (version in LocalRootProject).value
  val assemblyFile = (assembly in LocalProject("pyrasterframes")).value
  val PyZipFile = (packageBin in (LocalProject("pyrasterframes"), config("Python"))).value

  val copiedFiles = Seq(assemblyFile, PyZipFile)
    .map(f => {
      val dest = wd / f.getName
      logger.info(s"Copying ${f.getName} to: " + dest)
      IO.copyFile(f, dest)
      dest
  })

  val imageName = "rasterframes-notebooks"
  val targetFile = wd / s"$imageName.tar"
  Process("docker-compose build", wd) ! logger
  Process(s"docker tag $imageName:latest $imageName:$ver", wd) ! logger
  //Process(s"docker save -o $targetFile $imageName:$ver") ! logger

  IO.delete(copiedFiles)
  logger.info("Removed copied artifacts")
  targetFile
}
