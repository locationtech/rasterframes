import com.typesafe.sbt.{GitPlugin, SbtGit}
import com.typesafe.sbt.SbtGit.git
import sbt.Keys._
import sbt._
import xerial.sbt.Sonatype.autoImport._

/**
 * @since 8/20/17
 */
object RFProjectPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires = GitPlugin
  object autoImport {
    val rfSparkVersion = settingKey[String]("Apache Spark version")
    val rfGeoTrellisVersion = settingKey[String]("GeoTrellis version")
    val rfGeoMesaVersion = settingKey[String]("GeoMesa version")

    def geotrellis(module: String) = Def.setting {
      "org.locationtech.geotrellis" %% s"geotrellis-$module" % rfGeoTrellisVersion.value
    }
    def spark(module: String) = Def.setting {
      "org.apache.spark" %% s"spark-$module" % rfSparkVersion.value
    }

    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.3" % Test
  }

  override def projectSettings = Seq(
    organization := "org.locationtech.rasterframes",
    organizationName := "LocationTech RasterFrames",
    startYear := Some(2017),
    homepage := Some(url("http://rasterframes.io")),
    git.remoteRepo := "git@github.com:locationtech/rasterframes.git",
    scmInfo := Some(ScmInfo(url("https://github.com/locationtech/rasterframes"), "git@github.com:locationtech/rasterframes.git")),
    description := "RasterFrames brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of GeoTrellis",
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    scalaVersion := "2.11.12",
    scalacOptions ++= Seq("-feature", "-deprecation"),
    scalacOptions in (Compile, doc) ++= Seq("-no-link-warnings"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    cancelable in Global := true,
    resolvers ++= Seq(
      "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
      "boundless-releases" at "https://repo.boundlessgeo.com/main/",
      "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools/"
    ),
    publishTo := sonatypePublishTo.value,
    publishMavenStyle := true,
    publishArtifact in (Compile, packageDoc) := true,
    publishArtifact in Test := false,
    fork in Test := true,
    javaOptions in Test := Seq("-Xmx2G", "-Djava.library.path=/usr/local/lib"),
    parallelExecution in Test := false,
    testOptions in Test += Tests.Argument("-oDF"),
    developers := List(
      Developer(
        id = "metasim",
        name = "Simeon H.K. Fitch",
        email = "fitch@astraea.io",
        url = url("http://www.astraea.io")
      ),
      Developer(
        id = "mteldridge",
        name = "Matt Eldridge",
        email = "meldridge@astraea.io",
        url = url("http://www.astraea.io")
      ),
      Developer(
        id = "bguseman",
        name = "Ben Guseman",
        email = "bguseman@astraea.io",
        url = url("http://www.astraea.io")
      )
    ),
    initialCommands in console :=
      """
        |import org.apache.spark._
        |import org.apache.spark.sql._
        |import org.apache.spark.sql.functions._
        |import geotrellis.raster._
        |import geotrellis.spark._
        |import astraea.spark.rasterframes._
        |implicit val spark = SparkSession.builder()
        |  .master("local[*]")
        |  .withKryoSerialization
        |  .getOrCreate()
        |  .withRasterFrames
        |spark.sparkContext.setLogLevel("ERROR")
        |import spark.implicits._
      """.stripMargin.trim,
    cleanupCommands in console := "spark.stop()"
  )
}
