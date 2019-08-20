addCommandAlias("pyDocs", "pyrasterframes/doc")
addCommandAlias("pyTest", "pyrasterframes/test")
addCommandAlias("pyBuild", "pyrasterframes/package")

exportJars := true
Python / doc / sourceDirectory := (Python / target).value / "docs"
Python / doc / target := (Python / target).value / "docs"
Python / doc := (Python / doc / target).toTask.dependsOn(
  Def.sequential(
    assembly,
    Test / compile,
    pySetup.toTask(" pweave")
  )
).value

doc := (Python / doc).value

val nbInclude = Def.setting[FileFilter](GlobFilter("*.ipynb"))

lazy val pyNotebooks = taskKey[Seq[File]]("Convert relevant scripts into notebooks")
pyNotebooks := {
  val _ = pySetup.toTask(" notebooks").value
  ((Python / doc / target).value ** "*.ipynb").get()
}

lazy val pySparkCmd = taskKey[Unit]("Create build and emit command to run in pyspark")
pySparkCmd := {
  val s = streams.value
  val jvm = assembly.value
  val py = (Python / packageBin).value
  val script = IO.createTemporaryDirectory / "pyrf_init.py"
  IO.write(script, """
import pyrasterframes
from pyrasterframes.rasterfunctions import *
""")
  val msg = s"PYTHONSTARTUP=$script pyspark --jars $jvm --py-files $py"
  s.log.debug(msg)
  println(msg)
}



