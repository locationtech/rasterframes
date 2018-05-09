enablePlugins(AssemblyPlugin)

moduleName := "rasterframes-workshop"

libraryDependencies ++= Seq(
  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided
)
