moduleName := "raster-frames-datasource"

libraryDependencies ++= Seq(
  geotrellis("s3").value,
  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided
)

// Run generateDocs to help convert examples to tut docs.
//docsMap := Map(baseDirectory.value / "src" / "test" -> target.value / "literator" )
