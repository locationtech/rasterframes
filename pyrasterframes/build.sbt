import PythonBuildPlugin.autoImport.pySetup


Test / pythonSource := (Compile / sourceDirectory).value / "tests"

exportJars := true

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

addCommandAlias("pyTest", "pyrasterframes/test")