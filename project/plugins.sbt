logLevel := sbt.Level.Error

resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")

addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "3.0.2")

addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.10")

addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.6.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.1")

// addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.6")

addSbtPlugin("com.servicerocket" % "sbt-git-flow" % "0.1.3-astraea.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.0")

addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.3.1")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.27")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
