enablePlugins(BenchmarkPlugin)

libraryDependencies ++= Seq(
  spark("core").value,
  spark("sql").value,
  geotrellis("spark").value,
  geotrellis("raster").value,
  "org.slf4j" % "slf4j-log4j12" % "1.7.16"
)

jmhIterations := Some(5)
jmhTimeUnit := None
javaOptions in Jmh := Seq("-Xmx4g")

// To enable profiling:
// jmhExtraOptions := Some("-prof jmh.extras.JFR")
// jmhExtraOptions := Some("-prof gc")

