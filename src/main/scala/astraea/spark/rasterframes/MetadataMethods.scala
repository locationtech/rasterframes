/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package astraea.spark.rasterframes
import geotrellis.util.MethodExtensions
import spray.json.{JsObject, JsonFormat}
import org.apache.spark.sql.types.{Metadata ⇒ SQLMetadata}

/**
 * Extension methods used for transforming the metadata in a ContextRDD.
 *
 * @author sfitch
 * @since 7/18/17
 */
abstract class MetadataMethods[M: JsonFormat] extends MethodExtensions[M] {
  def asColumnMetadata: SQLMetadata = {
    val fmt = implicitly[JsonFormat[M]]
    fmt.write(self) match {
      case s: JsObject ⇒ SQLMetadata.fromJson(s.compactPrint)
      case _ ⇒ SQLMetadata.empty
    }
  }
}
