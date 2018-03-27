enablePlugins(BenchmarkPlugin)

libraryDependencies ++= Seq(
  spark("core"),
  spark("sql"),
  geotrellis("spark"),
  geotrellis("raster")
)

jmhIterations := Some(5)
jmhTimeUnit := None
javaOptions in Jmh := Seq("-Xmx4g")

// To enable profiling:
// jmhExtraOptions := Some("-prof jmh.extras.JFR")
// jmhExtraOptions := Some("-prof gc")

