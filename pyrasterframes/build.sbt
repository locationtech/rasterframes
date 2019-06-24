addCommandAlias("pyDocs", "pyrasterframes/doc")
addCommandAlias("pyTest", "pyrasterframes/test")
addCommandAlias("pyBuild", "pyrasterframes/package")

Test / pythonSource := (Compile / sourceDirectory).value / "tests"

exportJars := true
Python / doc / sourceDirectory := (Python / target).value / "docs"
Python / doc / target := (Python / target).value / "docs"
  //(Compile / target).value / "py-markdown"
Python / doc := (Python / doc / target).toTask.dependsOn(
  Def.sequential(
    assembly,
    Test / compile,
    pySetup.toTask(" pweave")
  )
).value

doc := (Python / doc).value

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



