enablePlugins(AssemblyPlugin)

moduleName := "adhoc"

libraryDependencies ++= Seq(
  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided
)

assemblySettings

mainClass in Compile := Some("astraea.spark.adhoc.MODISNDVI")