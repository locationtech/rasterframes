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

import sbt.Keys.{`package`, _}
import sbt._
import complete.DefaultParsers._
import sbt.Tests.Summary
import sbt.util.CacheStore

import scala.sys.process.Process
import sbtassembly.AssemblyPlugin.autoImport.assembly

object PythonBuildPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires = RFAssemblyPlugin

  object autoImport {
    val Python = config("python")
    val pythonCommand = settingKey[String]("Python command. Defaults to 'python'")
    val pySetup = inputKey[Int]("Run 'python setup.py <args>'. Returns exit code.")
    val pyWhl = taskKey[File]("Builds the Python wheel distribution")
    val maven2PEP440: String => String = {
      case VersionNumber(numbers, tags, extras) =>
        if (numbers.isEmpty) throw new MessageOnlyException("Version string is not convertible to PEP440.")

        // Reconstruct the primary version number
        val base = numbers.mkString(".")

        // Process items after the `-`. Due to PEP 440 constraints, some tags get converted
        // to local version suffixes, while others map directly to prerelease suffixes.
        val rc = "^[Rr][Cc](\\d+)$".r
        val tag = tags match {
          case Seq("SNAPSHOT") => ".dev"
          case Seq(rc(num)) => ".rc" + num
          case Seq(other) => ".dev+" + other
          case many @ Seq(_, _) => ".dev+" + many.mkString(".")
          case _ => ""
        }

        // sbt "extras" most closely map to PEP 440 local version suffixes.
        // The local version components are separated by `.`, preceded by a single `+`, and not multiple `+` as in sbt.
        // These next two expressions do the appropriate separator conversions while concatenating the components.
        val ssep = if (tag.contains("+")) "." else "+"
        val ext = if (extras.nonEmpty)
          extras.map(_.replaceAllLiterally("+", "")).mkString(ssep, ".", "")
        else ""

        base + tag + ext
    }
  }
  import autoImport._

  val copyPySources = Def.task {
    val log = streams.value.log
    val destDir = (Python / target).value
    val cacheDir = streams.value.cacheDirectory
    val maps =  (Python / mappings).value
    val resolved = maps map { case (file, d) => (file, destDir / d) }
    log.info(s"Synchronizing ${maps.size} files to '${destDir}'")
    Sync.sync(CacheStore(cacheDir / "python"))(resolved)
    destDir
  }

  val pyWhlJar = Def.task {
    val log = streams.value.log
    val buildDir = (Python / target).value
    val asmbl = (Compile / assembly).value
    val dest = buildDir / "deps" / "jars" / asmbl.getName
    IO.copyFile(asmbl, dest)
    log.info(s"PyRasterFrames assembly written to '$dest'")
    dest
  }.dependsOn(copyPySources)

  val pyWhlImp = Def.task {
    val log = streams.value.log
    val buildDir = (Python / target).value

    val jars = (buildDir / "deps" / "jars" ** "*.jar").get()
    if (jars.size > 1) {
      throw new MessageOnlyException("Two assemblies found in the package. Run 'clean'.\n" + jars.mkString(", "))
    }

    val retcode = pySetup.toTask(" build bdist_wheel").value
    if(retcode != 0) throw new MessageOnlyException(s"'python setup.py' returned $retcode")
    val whls = (buildDir / "dist" ** "pyrasterframes*.whl").get()
    require(whls.length == 1, s"Running setup.py should have produced a single .whl file. Found $whls")
    log.info(s"Python .whl file written to '${whls.head}'")
    whls.head
  }.dependsOn(pyWhlJar)

  val pyWhlAsZip = Def.task {
    val log = streams.value.log
    val pyDest = (packageBin / artifactPath).value
    val whl = pyWhl.value
    IO.copyFile(whl, pyDest)
    log.info(s"Maven Python artifact written to '$pyDest'")
    pyDest
  }.dependsOn(pyWhl)

  override def projectConfigurations: Seq[Configuration] = Seq(Python)

  override def projectSettings = Seq(
    assembly / test := {},
    pythonCommand := "python",
    pySetup := {
      val s = streams.value
      val wd = copyPySources.value
      val args = spaceDelimited("<args>").parsed
      val cmd = Seq(pythonCommand.value, "setup.py") ++ args
      val ver = (Python / version).value
      s.log.info(s"Running '${cmd.mkString(" ")}' in '$wd'")
      val ec = Process(cmd, wd, "RASTERFRAMES_VERSION" -> ver).!
      if (ec != 0)
        throw new MessageOnlyException(s"'${cmd.mkString(" ")}' exited with value '$ec'")
      ec
    },
    pyWhl := pyWhlImp.value,
    Compile / `package` := (Compile / `package`).dependsOn(Python / packageBin).value,
    Test / testQuick := (Python / testQuick).evaluated,
    Test / executeTests := {
      val standard = (Test / executeTests).value
      standard.overall match {
        case TestResult.Passed =>
          (Python / executeTests).value
        case _ =>
          val pySummary = Summary("pyrasterframes", "tests skipped due to scalatest failures")
          standard.copy(summaries = standard.summaries ++ Iterable(pySummary))
      }
    }
  ) ++
    inConfig(Python)(Seq(
      sourceDirectory := (Compile / sourceDirectory).value / "python",
      sourceDirectories := Seq((Python / sourceDirectory).value),
      version ~= maven2PEP440,
      target := (Compile / target).value / "python",
      includeFilter := "*",
      excludeFilter := HiddenFileFilter || "__pycache__" || "*.egg-info",
      sources := Defaults.collectFiles(Python / sourceDirectories, Python / includeFilter, Python / excludeFilter).value,
      mappings := Defaults.relativeMappings(Python / sources, Python / sourceDirectories).value,
      packageBin := Def.sequential(
        Compile / packageBin,
        pyWhl,
        pyWhlAsZip,
      ).value,
      packageBin / artifact := {
        val java = (Compile / packageBin / artifact).value
        java.withType("zip").withClassifier(Some("python")).withExtension("zip")
      },
      packageBin / artifactPath := {
        val dest = (Compile / packageBin / artifactPath).value.getParentFile
        val art = (Python / packageBin / artifact).value
        val ver = version.value
        dest / s"${art.name}-$ver-py3-none-any.whl"
      },
      testQuick := pySetup.toTask(" test"),
      executeTests := Def.task {
        val resultCode = pySetup.toTask(" test").value
        val msg = resultCode match {
          case 1 => "There are Python test failures."
          case 2 => "Python test execution was interrupted."
          case 3 => "Internal error during Python test execution."
          case 4 => "PyTest usage error."
          case 5 => "No Python tests found."
          case x if x != 0 => "Unknown error while running Python tests."
          case _ => "PyRasterFrames tests successfully completed."
        }
        val pySummary = Summary("pyrasterframes", msg)
        // Would be cool to derive this from the python output...
        val result = if (resultCode == 0) {
          new SuiteResult(
            TestResult.Passed,
            passedCount = 1,
            failureCount = 0,
            errorCount = 0,
            skippedCount = 0,
            ignoredCount = 0,
            canceledCount = 0,
            pendingCount = 0
          )
        }
        else {
          new SuiteResult(
            TestResult.Failed,
            passedCount = 0,
            failureCount = 1,
            errorCount = 0,
            skippedCount = 0,
            ignoredCount = 0,
            canceledCount = 0,
            pendingCount = 0
          )
        }

        Tests.Output(result.result, Map("Python Tests" -> result), Iterable(pySummary))
      }.dependsOn(assembly).value
    )) ++
    addArtifact(Python / packageBin / artifact, Python / packageBin)
}
