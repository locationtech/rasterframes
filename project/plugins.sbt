logLevel := sbt.Level.Error

addDependencyTreePlugin
addSbtPlugin("com.eed3si9n"          % "sbt-assembly"               % "1.2.0")
addSbtPlugin("com.eed3si9n"          % "sbt-buildinfo"              % "0.11.0")
addSbtPlugin("com.eed3si9n"          % "sbt-unidoc"                 % "0.4.1")
addSbtPlugin("de.heikoseeberger"     % "sbt-header"                 % "3.0.2")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox"                % "0.5.5")
addSbtPlugin("io.github.jonas"       % "sbt-paradox-material-theme" % "0.6.0")
addSbtPlugin("pl.project13.scala"    % "sbt-jmh"                    % "0.3.6")
addSbtPlugin("net.vonbuchholtz"      % "sbt-dependency-check"       % "0.2.10")
addSbtPlugin("com.typesafe.sbt"      % "sbt-native-packager"        % "1.3.19")
addSbtPlugin("org.scalameta"         % "sbt-scalafmt"               % "2.4.3")

addSbtPlugin("com.github.sbt"        % "sbt-ghpages"                % "0.7.0")
addSbtPlugin("com.dwijnand"          % "sbt-dynver"                 % "4.1.1")
addSbtPlugin("org.xerial.sbt"        % "sbt-sonatype"               % "3.9.17")
addSbtPlugin("com.github.sbt"        % "sbt-pgp"                    % "2.2.1")
addSbtPlugin("com.github.sbt"        % "sbt-ci-release"             % "1.5.11")
