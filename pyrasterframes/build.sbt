Compile / pythonSource := baseDirectory.value / "python"
Test / pythonSource := baseDirectory.value / "python" / "tests"

addCommandAlias("pyExamples", "pySetup examples")