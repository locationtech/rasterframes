import PythonBuildPlugin.autoImport.pySetup

Compile / pythonSource := baseDirectory.value / "python"
Test / pythonSource := baseDirectory.value / "python" / "tests"

lazy val pyExamples = taskKey[Unit]("Run examples")

pyExamples := Def.sequential(
  assembly,
  pySetup.toTask(" examples")
).value

addCommandAlias("pyTest", "pyrasterframes/test")