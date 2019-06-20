import PythonBuildPlugin.autoImport._

addCommandAlias("pyTest", "pyrasterframes/test")
addCommandAlias("pyDocs", "pyrasterframes/doc")


Test / pythonSource := (Compile / sourceDirectory).value / "tests"

exportJars := true
Python / doc / sourceDirectory := (Python / target).value / "docs"
Python / doc / target := (Python / target).value / "markdown" / "pyrasterframes"
Python / doc := {
  val _ = Def.sequential(
    assembly,
    pySetup.toTask(" pweave"),
    copySources(
      Python / doc / sourceDirectory,
      Python / doc / target,
      deleteFirst = true
    )
  ).value
  (Python / doc / target).value
}

doc := (Python / doc).value

lazy val pySparkCmd = taskKey[Unit]("Create build and emit command to run in pyspark")
pySparkCmd := {
  val s = streams.value
  val jvm = assembly.value
  val py = (Python / packageBin).value
  val script = IO.createTemporaryDirectory / "pyrf_init.py"
  IO.write(script, """
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
""")
  val msg = s"PYTHONSTARTUP=$script pyspark --jars $jvm --py-files $py"
  s.log.debug(msg)
  println(msg)
}

lazy val pyExamples = taskKey[Unit]("Run python examples")

pyExamples := Def.sequential(
  assembly,
  pySetup.toTask(" examples")
).value

