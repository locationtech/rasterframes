enablePlugins(BenchmarkPlugin)

jmhIterations := Some(5)
jmhTimeUnit := None
javaOptions in Jmh := Seq("-Xmx4g")

// To enable profiling:
// jmhExtraOptions := Some("-prof jmh.extras.JFR")

