addCommandAlias("pyDocs", "pyrasterframes/doc")
addCommandAlias("pyTest", "pyrasterframes/test")
addCommandAlias("pyBuild", "pyrasterframes/package")
addCommandAlias("pyExamples", "pyrasterframes/run")

Test / pythonSource := (Compile / sourceDirectory).value / "tests"

Compile / run := pySetup.toTask(" examples").dependsOn(assembly).value

//RFProjectPlugin.IntegrationTest / test := (Compile / run).inputTaskValue

exportJars := true
Python / doc / sourceDirectory := (Python / target).value / "docs"
Python / doc / target := (Compile / target).value / "py-markdown"
Python / doc := (Python / doc / target).toTask.dependsOn(
  Def.sequential(
    assembly,
    pySetup.toTask(" pweave"),
    copySources(
      Python / doc / sourceDirectory,
      Python / doc / target,
      deleteFirst = true
    )
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
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
""")
  val msg = s"PYTHONSTARTUP=$script pyspark --jars $jvm --py-files $py"
  s.log.debug(msg)
  println(msg)
}



