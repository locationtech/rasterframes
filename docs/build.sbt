import com.typesafe.sbt.SbtGit.git

enablePlugins(SiteScaladocPlugin, ParadoxPlugin, TutPlugin, GhpagesPlugin, ScalaUnidocPlugin)

name := "rasterframes-docs"

libraryDependencies ++= Seq(
  spark("mllib").value % Tut,
  spark("sql").value % Tut
)

git.remoteRepo := "git@github.com:locationtech/rasterframes.git"
apiURL := Some(url("http://rasterframes.io/latest/api"))
autoAPIMappings := true
ghpagesNoJekyll := true

ScalaUnidoc / siteSubdirName := "latest/api"
paradox / siteSubdirName := "."

addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, ScalaUnidoc / siteSubdirName)
addMappingsToSiteDir(Compile / paradox / mappings, paradox / siteSubdirName)

paradoxProperties ++= Map(
  "github.base_url" -> "https://github.com/locationtech/rasterframes",
  "version" -> version.value,
  "scaladoc.org.apache.spark.sql.gt" -> "http://rasterframes.io/latest"
  //"scaladoc.geotrellis.base_url" -> "https://geotrellis.github.io/scaladocs/latest",
  // "snip.pyexamples.base_dir" -> (baseDirectory.value + "/../pyrasterframes/python/test/examples")
)
paradoxTheme := Some(builtinParadoxTheme("generic"))
//paradoxTheme / sourceDirectory := sourceDirectory.value / "main" / "paradox" / "_template"

Compile / doc / scalacOptions++= Seq( "-J-Xmx6G", "-no-link-warnings")

Tut / run / fork := true

Tut / run / javaOptions := Seq("-Xmx8G", "-Dspark.ui.enabled=false")

val skipTut = false

if (skipTut) Seq(
  Compile / paradox / sourceDirectory := tutSourceDirectory.value,
  makeSite := makeSite.dependsOn(Compile / unidoc).value
)
else Seq(
  Compile / paradox := (Compile / paradox).dependsOn(tutQuick).value,
  Compile / paradox / sourceDirectory := tutTargetDirectory.value,
  makeSite := makeSite.dependsOn(Compile / unidoc).value
)
