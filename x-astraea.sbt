// Internal Astraea-specific overides
version in ThisBuild := "0.8.0-astraea-SNAPSHOT"
credentials in ThisBuild += Credentials(Path.userHome / ".sbt" / ".credentials")
publishTo := {
  val base = "https://s22s.mycloudrepo.io/repositories"
  if (isSnapshot.value)
    Some("Astraea Internal Snapshots" at s"$base/snapshots/")
  else
    Some("Astraea Internal Releases" at s"$base/releases/")
}