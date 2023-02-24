import com.github.sbt.git.{GitPlugin, SbtGit}
import com.github.sbt.git.SbtGit.git
import sbt.Keys._
import sbt._
import xerial.sbt.Sonatype.autoImport._

/**
 * @since 8/20/17
 */
object RFProjectPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires = GitPlugin && RFDependenciesPlugin

  override def projectSettings = Seq(
    organization := "org.locationtech.rasterframes",
    organizationName := "LocationTech RasterFrames",
    startYear := Some(2017),
    homepage := Some(url("http://rasterframes.io")),
    git.remoteRepo := "git@github.com:locationtech/rasterframes.git",
    scmInfo := Some(ScmInfo(url("https://github.com/locationtech/rasterframes"), "git@github.com:locationtech/rasterframes.git")),
    description := "RasterFrames brings the power of Spark DataFrames to geospatial raster data.",
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    scalaVersion := "2.12.17",
    scalacOptions ++= Seq(
      "-target:jvm-1.8",
      "-feature",
      "-language:higherKinds",
      "-deprecation",
      "-Ywarn-dead-code",
      "-Ywarn-unused-import"
    ),
    Compile / doc / scalacOptions ++= Seq("-no-link-warnings"),
    Compile / console / scalacOptions := Seq("-feature"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    initialize := {
      val _ = initialize.value // run the previous initialization
      val sparkVer = VersionNumber(RFDependenciesPlugin.autoImport.rfSparkVersion.value)
      if (sparkVer.matchesSemVer(SemanticSelector("<3.0"))) {
        val curr = VersionNumber(sys.props("java.specification.version"))
        val req = SemanticSelector("=1.8")
        assert(curr.matchesSemVer(req), s"Java $req required for $sparkVer. Found $curr.")
      }
    },
    Global / cancelable := true,
    Compile / packageDoc / publishArtifact := true,
    Test / publishArtifact := false,
    // don't fork it in tests to reduce memory usage
    Test / fork := false,
    // Test / javaOptions ++= Seq(
      // "-XX:+HeapDumpOnOutOfMemoryError",
      // "-XX:HeapDumpPath=/tmp"
    // ),
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
    developers := List(
      Developer(
        id = "metasim",
        name = "Simeon H.K. Fitch",
        email = "fitch@astraea.earth",
        url = url("https://github.com/metasim")
      ),
      Developer(
        id = "vpipkt",
        name = "Jason Brown",
        email = "jbrown@astraea.earth",
        url = url("https://github.com/vpipkt")
      ),
      Developer(
        id = "echeipesh",
        name = "Eugene Cheipesh",
        email = "echeipesh@gmail.com",
        url = url("https://github.com/echeipesh")
      ),
      Developer(
        id = "pomadchin",
        name = "Grigory Pomadchin",
        email = "gpomadchin@azavea.com",
        url = url("https://github.com/pomadchin")
      ),
      Developer(
        id = "bguseman",
        name = "Ben Guseman",
        email = "bguseman@astraea.earth",
        url = url("http://www.astraea.earth")
      )
    ),
    console / initialCommands :=
      """
        |import org.apache.spark._
        |import org.apache.spark.sql._
        |import org.apache.spark.sql.functions._
        |import geotrellis.raster._
        |import geotrellis.spark._
        |import org.locationtech.rasterframes._
        |implicit val spark = SparkSession.builder()
        |  .master("local[*]")
        |  .withKryoSerialization
        |  .getOrCreate()
        |  .withRasterFrames
        |spark.sparkContext.setLogLevel("ERROR")
        |import spark.implicits._
      """.stripMargin.trim,
    console / cleanupCommands := "spark.stop()"
  )
}
