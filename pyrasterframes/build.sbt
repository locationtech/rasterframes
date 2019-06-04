import PythonBuildPlugin.autoImport.pySetup

Compile / pythonSource := baseDirectory.value / "python"
Test / pythonSource := baseDirectory.value / "python" / "tests"

// This setting adds relevant python source to the JVM artifact
// where pyspark can pick it up from the `--packages` setting.

Python / sourceDirectories := Seq(
  (Compile / pythonSource).value / "pyrasterframes",
  (Compile / pythonSource).value / "geomesa_pyspark"
)

Compile / packageBin / mappings ++= {
  val pybase = (Compile / pythonSource).value
  val dirs = (Python / sourceDirectories).value
  for {
    d <- dirs
    p <- d ** "*.py" --- d pair (f => IO.relativize(pybase, f))
  } yield p
}

// This is needed for the above to get included properly in the assembly.
exportJars := true

lazy val pyExamples = taskKey[Unit]("Run python examples")

pyExamples := Def.sequential(
  assembly,
  pySetup.toTask(" examples")
).value

addCommandAlias("pyTest", "pyrasterframes/test")