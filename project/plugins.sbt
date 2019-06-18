logLevel := sbt.Level.Error

addSbtCoursier
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "3.0.2")
addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.6.10")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.2")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.5.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.6")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.1")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.1")
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "0.2.10")
addSbtPlugin("com.github.gseitz" %% "sbt-release" % "1.0.9")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.19")


