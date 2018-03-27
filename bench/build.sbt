enablePlugins(BenchmarkPlugin)

libraryDependencies ++= Seq(
  spark("core").value,
  spark("sql").value,
  geotrellis("spark").value,
  geotrellis("raster").value
)

jmhIterations := Some(5)
jmhTimeUnit := None
javaOptions in Jmh := Seq("-Xmx4g")

// To enable profiling:
// jmhExtraOptions := Some("-prof jmh.extras.JFR")
// jmhExtraOptions := Some("-prof gc")

