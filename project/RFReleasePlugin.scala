
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
import com.servicerocket.sbt.release.git.flow.Steps._
import com.typesafe.sbt.sbtghpages.GhpagesPlugin
import com.typesafe.sbt.sbtghpages.GhpagesPlugin.autoImport.ghpagesPushSite
import com.typesafe.sbt.site.SitePlugin
import com.typesafe.sbt.site.SitePlugin.autoImport.makeSite
object RFReleasePlugin extends AutoPlugin {
  override def trigger: PluginTrigger = noTrigger
  override def requires = RFProjectPlugin && SitePlugin && GhpagesPlugin
  override def projectSettings = {
    val buildSite: State ⇒ State = releaseStepTask(makeSite in LocalProject("docs"))
    val publishSite: State ⇒ State = releaseStepTask(ghpagesPushSite in LocalProject("docs"))
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
