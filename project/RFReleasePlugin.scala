
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

import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._
import com.typesafe.sbt.sbtghpages.GhpagesPlugin
import com.typesafe.sbt.sbtghpages.GhpagesPlugin.autoImport.ghpagesPushSite
import com.typesafe.sbt.site.SitePlugin
import com.typesafe.sbt.site.SitePlugin.autoImport.makeSite
import scala.sys.process.{Process => SProcess}

/** Release process support. */
object RFReleasePlugin extends AutoPlugin {
  override def trigger: PluginTrigger = noTrigger
  override def requires = RFProjectPlugin && SitePlugin && GhpagesPlugin
  override def projectSettings = {
    val buildSite: State => State = releaseStepTask(LocalProject("docs") / makeSite)
    val publishSite: State => State = releaseStepTask(LocalProject("docs") / ghpagesPushSite)
    Seq(
      releaseIgnoreUntrackedFiles := true,
      releaseTagName := s"${version.value}",
      releaseProcess := Seq[ReleaseStep](
        checkSnapshotDependencies,
        checkGitFlowExists,
        inquireVersions,
        runClean,
        runTest,
        gitFlowReleaseStart,
        setReleaseVersion,
        buildSite,
        commitReleaseVersion,
        tagRelease,
        releaseStepCommand("publishSigned"),
        releaseStepCommand("sonatypeReleaseAll"),
        publishSite,
        gitFlowReleaseFinish,
        setNextVersion,
        commitNextVersion,
        remindMeToPush
      ),
      commands += Command.command("bumpVersion"){ st =>
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

  def releaseVersion(state: State): String =
    state.get(ReleaseKeys.versions).map(_._1).getOrElse {
      sys.error("No versions are set! Was this release part executed before inquireVersions?")
    }

  val gitFlowReleaseStart = ReleaseStep(state => {
    val version = releaseVersion(state)
    SProcess(Seq("git", "flow", "release", "start", version)).!
    state
  })

  val gitFlowReleaseFinish = ReleaseStep(state => {
    val version = releaseVersion(state)
    SProcess(Seq("git", "flow", "release", "finish", "-n", s"$version")).!
    state
  })

  val remindMeToPush = ReleaseStep(state => {
    state.log.warn("Don't forget to git push master AND develop!")
    state
  })

  val checkGitFlowExists = ReleaseStep(state => {
    SProcess(Seq("command", "-v", "git-flow")).!! match {
      case "" => sys.error("git-flow is required for release. See https://github.com/nvie/gitflow for installation instructions.")
      case _ => SProcess(Seq("git", "flow", "init", "-d")).! match {
        case 0 => state
        case e => sys.error(s"git-flow init failed with error code $e")
      }
    }
  })
}
