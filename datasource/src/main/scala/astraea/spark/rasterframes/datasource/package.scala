/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes

import java.net.URI

import org.apache.spark.sql.sources.{And, Filter}

import scala.util.Try

/**
 * Module utilities
 *
 * @since 1/13/18
 */
package object datasource {

  private[rasterframes]
  def numParam(key: String, parameters: Map[String, String]): Option[Long] =
    parameters.get(key).map(_.toLong)

  private[rasterframes]
  def uriParam(key: String, parameters: Map[String, String]) =
    parameters.get(key).flatMap(p ⇒ Try(URI.create(p)).toOption)

}
