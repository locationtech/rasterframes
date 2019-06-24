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

import sbt.KeyRanks.ASetting
import sbt.Keys.{`package`, _}
import sbt._
import complete.DefaultParsers._
import sbt.Tests.Summary

import scala.sys.process.Process
import sbtassembly.AssemblyPlugin.autoImport.assembly

object PythonBuildPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires = RFAssemblyPlugin

  object autoImport {
    val Python = config("python")
    val pythonSource = settingKey[File]("Default Python source directory.").withRank(ASetting)
    val pythonCommand = settingKey[String]("Python command. Defaults to 'python'")
    val pySetup = inputKey[Int]("Run 'python setup.py <args>'. Returns exit code.")

    // TODO: figure out how to rewrite this using the standard `mappings` facility.
    def copySources(srcDir: SettingKey[File], destDir: SettingKey[File], deleteFirst: Boolean) = Def.task {
      val s = streams.value
      val src = srcDir.value
      val dest = destDir.value
      if (deleteFirst)
        IO.delete(dest)
      dest.mkdirs()
      s.log.info(s"Copying '$src' to '$dest'")
      IO.copyDirectory(src, dest)
      dest
    }
  }
  import autoImport._

  val copyPySources = Def.sequential(
    copySources(Compile / pythonSource, Python / target, true),
    copySources(Test / pythonSource, Python / test / target, false)
  )
  
  val pyWhlJar = Def.task {
    val log = streams.value.log
    val buildDir = (Python / target).value
    val asmbl = (Compile / assembly).value
    val dest = buildDir / "deps" / "jars" / asmbl.getName
    IO.copyFile(asmbl, dest)
    log.info(s"PyRasterFrames assembly written to '$dest'")
    dest
  }

  val pyWhl = Def.task {
    val log = streams.value.log
    val buildDir = (Python / target).value
    val retcode = pySetup.toTask(" build bdist_wheel").value
    if(retcode != 0) throw new RuntimeException(s"'python setup.py' returned $retcode")
    val whls = (buildDir / "dist" ** "pyrasterframes*.whl").get()
    require(whls.length == 1, "Running setup.py should have produced a single .whl file. Try running `clean` first.")
    log.info(s"Python .whl file written to '${whls.head}'")
    whls.head
  }.dependsOn(pyWhlJar)

  val pyWhlAsZip = Def.task {
    val log = streams.value.log
    val pyDest = (packageBin / artifactPath).value
    val whl = pyWhl.value
    IO.copyFile(whl, pyDest)
    log.info(s"Maven Python .zip artifact written to '$pyDest'")
    pyDest
  }

  override def projectConfigurations: Seq[Configuration] = Seq(Python)

  override def projectSettings = Seq(
    assembly / test := {},
    pythonCommand := "python",
    pySetup := {
      val s = streams.value
      val _ = copyPySources.value
      val wd = (Python / target).value
      val args = spaceDelimited("<args>").parsed
      val cmd = Seq(pythonCommand.value, "setup.py") ++ args
      val ver = version.value
      s.log.info(s"Running '${cmd.mkString(" ")}' in '$wd'")
      Process(cmd, wd, "RASTERFRAMES_VERSION" -> ver).!
    },
    Compile / pythonSource := (Compile / sourceDirectory).value / "python",
    Test / pythonSource := (Test / sourceDirectory).value / "python",
    Compile / `package` := (Compile / `package`).dependsOn(Python / packageBin).value,
    Test / testQuick := (Python / testQuick).evaluated,
    Test / executeTests := {
      val standard = (Test / executeTests).value
      standard.overall match {
        case TestResult.Passed =>
          (Python / executeTests).value
        case _ ⇒
          val pySummary = Summary("pyrasterframes", "tests skipped due to scalatest failures")
          standard.copy(summaries = standard.summaries ++ Iterable(pySummary))
      }
    }
  ) ++
    inConfig(Python)(Seq(
      target := (Compile / target).value / "python",
      test / target := (Compile / target).value / "python" / "tests",
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
        dest / s"${art.name}-python-$ver.zip"
      },
      testQuick := pySetup.toTask(" test").value,
      executeTests := Def.task {
        val resultCode = pySetup.toTask(" test").value
        val msg = resultCode match {
          case 1 ⇒ "There are Python test failures."
          case 2 ⇒ "Python test execution was interrupted."
          case 3 ⇒ "Internal error during Python test execution."
          case 4 ⇒ "PyTest usage error."
          case 5 ⇒ "No Python tests found."
          case x if x != 0 ⇒ "Unknown error while running Python tests."
          case _ ⇒ "PyRasterFrames tests successfully completed."
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
        result
        Tests.Output(result.result, Map("PyRasterFramesTests" -> result), Iterable(pySummary))
      }.dependsOn(assembly).value
    )) ++
    addArtifact(Python / packageBin / artifact, Python / packageBin)
}
