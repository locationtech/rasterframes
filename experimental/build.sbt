moduleName := "rasterframes-experimental"

libraryDependencies ++= Seq(
  geotrellis("s3").value,
  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided
)

fork in IntegrationTest := true
javaOptions in IntegrationTest := Seq("-Xmx2G")
parallelExecution in IntegrationTest := false
