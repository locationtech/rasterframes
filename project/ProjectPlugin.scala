import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.AssemblyPlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._

import com.servicerocket.sbt.release.git.flow.Steps._
import com.typesafe.sbt.sbtghpages.GhpagesPlugin.autoImport._
import com.typesafe.sbt.site.SitePlugin.autoImport._
import xerial.sbt.Sonatype.autoImport._

/**
 * @since 8/20/17
 */
object ProjectPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  import autoImport._

  override def projectSettings = Seq(
    //organization := "org.locationtech.rasterframes",
    organization := "io.astraea",
    organizationName := "LocationTech RasterFrames",
    startYear := Some(2017),
    homepage := Some(url("http://rasterframes.io")),
    scmInfo := Some(ScmInfo(url("https://github.com/locationtech/rasterframes"), "git@github.com:locationtech/rasterframes.git")),
    description := "RasterFrames brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of GeoTrellis",
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    scalaVersion := "2.11.12",
    scalacOptions ++= Seq("-feature", "-deprecation"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    cancelable in Global := true,
    resolvers ++= Seq(
      "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
      "boundless-releases" at "https://repo.boundlessgeo.com/main/"
    ),

    rfSparkVersion in ThisBuild := "2.2.1" ,
    rfGeoTrellisVersion in ThisBuild := "1.2.0",
    rfGeoMesaVersion in ThisBuild := "2.0.0-rc.1",

    publishTo := sonatypePublishTo.value,
    publishMavenStyle := true,
    publishArtifact in (Compile, packageDoc) := true,
    publishArtifact in Test := false,
    fork in Test := true,
    javaOptions in Test := Seq("-Xmx2G"),
    parallelExecution in Test := false,
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
    )
  )

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

    def assemblySettings: Seq[Def.Setting[_]] = Seq(
      test in assembly := {},
      assemblyMergeStrategy in assembly := {
        case "logback.xml" ⇒ MergeStrategy.singleOrError
        case "git.properties" ⇒ MergeStrategy.discard
        case x if Assembly.isConfigFile(x) ⇒ MergeStrategy.concat
        case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) ⇒
          MergeStrategy.rename
        case PathList("META-INF", xs @ _*) ⇒
          xs map {_.toLowerCase} match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) ⇒
              MergeStrategy.discard
            case ps @ (x :: _) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") ⇒
              MergeStrategy.discard
            case "plexus" :: _ ⇒
              MergeStrategy.discard
            case "services" :: _ ⇒
              MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) ⇒
              MergeStrategy.filterDistinctLines
            case ("maven" :: rest ) if rest.lastOption.exists(_.startsWith("pom")) ⇒
              MergeStrategy.discard
            case _ ⇒ MergeStrategy.deduplicate
          }

        case _ ⇒ MergeStrategy.deduplicate
      }
    )

    def releaseSettings: Seq[Def.Setting[_]] = {
      val buildSite: (State) ⇒ State = releaseStepTask(makeSite in LocalProject("docs"))
      val publishSite: (State) ⇒ State = releaseStepTask(ghpagesPushSite in LocalProject("docs"))
      Seq(
        releaseIgnoreUntrackedFiles := true,
        releaseTagName := s"${version.value}",
        releaseProcess := Seq[ReleaseStep](
          checkSnapshotDependencies,
          checkGitFlowExists,
          inquireVersions,
          runTest,
          gitFlowReleaseStart,
          setReleaseVersion,
          buildSite,
          publishSite,
          commitReleaseVersion,
          tagRelease,
          releaseStepCommand("publishSigned"),
          releaseStepCommand("sonatypeReleaseAll"),
          gitFlowReleaseFinish,
          setNextVersion,
          commitNextVersion
        ),
        commands += Command.command("bumpVersion"){ st ⇒
          val extracted = Project.extract(st)
          val ver = extracted.get(version)
          val nextFun = extracted.runTask(releaseNextVersion, st)._2

          val nextVersion = nextFun(ver)

          val file = extracted.get(releaseVersionFile)
          IO.writeLines(file, Seq(s"""version in ThisBuild := "$nextVersion""""))
          extracted.appendWithSession(Seq(version := nextVersion), st)
        }
      )
    }
  }
}
