/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package astraea.spark.rasterframes.ml

import java.nio.file.Files

import astraea.spark.rasterframes._
import org.scalatest.BeforeAndAfter

/**
 * @author sfitch
 * @since 11/21/17
 */
class NoDataFilterSpec extends TestEnvironment with TestData with BeforeAndAfter {
  before {
    val _ = spark
  }

  describe("NoDataFilter trasformer") {
    it("should (de)serialize") {
      val ndf = new NoDataFilter()
      ndf.setInputCols(Array("a", "b", "c"))
      val dest = Files.createTempFile("rf", ".json").toUri.toASCIIString

      ndf.write.overwrite().save(dest)

      val ndf2 = NoDataFilter.load(dest)

      assert(ndf.getInputCols === ndf2.getInputCols)
    }
  }

  protected def withFixture(test: Any) = ???
}
