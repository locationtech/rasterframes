enablePlugins(BuildInfoPlugin)

moduleName := "rasterframes"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.2",
  "org.locationtech.geomesa" %% "geomesa-z3" % rfGeoMesaVersion.value,
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % rfGeoMesaVersion.value exclude("jgridshift", "jgridshift"),

  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided,
  geotrellis("spark").value,
  geotrellis("raster").value,
  geotrellis("s3").value,
  geotrellis("spark-testkit").value % Test excludeAll (
    ExclusionRule(organization = "org.scalastic"),
    ExclusionRule(organization = "org.scalatest")
  ),
  scalaTest
)

buildInfoKeys ++= Seq[BuildInfoKey](
  name, version, scalaVersion, sbtVersion, rfGeoTrellisVersion, rfGeoMesaVersion, rfSparkVersion
)

buildInfoPackage := "astraea.spark.rasterframes"

buildInfoObject := "RFBuildInfo"

buildInfoOptions := Seq(
  BuildInfoOption.ToMap,
  BuildInfoOption.BuildTime
)

