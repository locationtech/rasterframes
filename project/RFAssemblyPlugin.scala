/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyPlugin.autoImport.{ShadeRule, _}

import scala.util.matching.Regex

/**
 * Standard support for creating assembly jars.
 */
object RFAssemblyPlugin extends AutoPlugin {
  override def requires = AssemblyPlugin && RFDependenciesPlugin

  implicit class RichRegex(val self: Regex) extends AnyVal {
    def =~(s: String) = self.pattern.matcher(s).matches
  }

  object autoImport {
    val assemblyExcludedJarPatterns = settingKey[Seq[Regex]](
      "List of regular expressions identifying jar file names that will be force-excluded from assembly"
    )
  }

  override def projectSettings = Seq(
    assembly / test := {},
    autoImport.assemblyExcludedJarPatterns := Seq(
      "scalatest.*".r,
      "junit.*".r
    ),
    assembly / assemblyShadeRules := {
      val shadePrefixes = Seq(
        "shapeless",
        "com.github.ben-manes.caffeine",
        "com.github.benmanes.caffeine",
        "com.github.mpilquist",
        "com.amazonaws",
        "org.apache.avro",
        "org.apache.http",
        "com.google.guava",
        "com.google.common",
        "com.typesafe.config",
        "com.fasterxml.jackson",
        "io.netty",
        "spire",
        "cats.kernel"
      )
      shadePrefixes.map(p => ShadeRule.rename(s"$p.**" -> s"shaded.rasterframes.$p.@1").inAll)
    },
    assembly / assemblyOption :=
      (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / assemblyOutputPath := (ThisBuild / baseDirectory).value / "dist" / s"${normalizedName.value}-assembly-${version.value}.jar",
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      val excludedJarPatterns = autoImport.assemblyExcludedJarPatterns.value
      cp filter { jar =>
        excludedJarPatterns
          .exists(_ =~ jar.data.getName)
      }
    },
    assembly / assemblyMergeStrategy := {
      case "logback.xml" => MergeStrategy.singleOrError
      case "git.properties" => MergeStrategy.discard
      // com.sun.activation % jakarta.activation % 1.2.2
      // org.threeten % threeten-extra % 1.6.0
      case "module-info.class" => MergeStrategy.discard
      case x if Assembly.isConfigFile(x) => MergeStrategy.concat
      case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
        MergeStrategy.rename
      case PathList("META-INF", xs@_*) =>
        xs map {
          _.toLowerCase
        } match {
          case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
            MergeStrategy.discard
          case "io.netty.versions.properties" :: Nil =>
            MergeStrategy.concat
          case ps@x :: _ if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
            MergeStrategy.discard
          case "plexus" :: _ =>
            MergeStrategy.discard
          case "services" :: _ =>
            MergeStrategy.filterDistinctLines
          case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
            MergeStrategy.filterDistinctLines
          case "maven" :: rest if rest.lastOption.exists(_.startsWith("pom")) =>
            MergeStrategy.discard
          case _ => MergeStrategy.deduplicate
        }

      case _ => MergeStrategy.deduplicate
    }
  )
}