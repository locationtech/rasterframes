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

package org.locationtech.rasterframes.model

import com.github.blemale.scaffeine.Scaffeine
import geotrellis.proj4.CRS
import org.locationtech.proj4j.CoordinateReferenceSystem
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.model.LazyCRS.EncodedCRS

class LazyCRS(val encoded: EncodedCRS) extends CRS {
  private lazy val delegate: CRS = LazyCRS.cache.get(encoded)
  override def proj4jCrs: CoordinateReferenceSystem = delegate.proj4jCrs
  override def toProj4String: String =
    if (encoded.startsWith("+proj")) encoded
    else delegate.toProj4String

  override def equals(o: Any): Boolean = o match {
    case l: LazyCRS =>
      encoded == l.encoded ||
        toProj4String == l.toProj4String ||
        super.equals(o)
    case c: CRS =>
      toProj4String == c.toProj4String ||
        delegate.equals(c)
    case _ => false
  }
}

object LazyCRS {
  trait ValidatedCRS
  type EncodedCRS = String with ValidatedCRS

  val wktKeywords = Seq("GEOGCS", "PROJCS", "GEOCCS")

  private object WKTCRS {
    def unapply(src: String): Option[CRS] =
      if (wktKeywords.exists { prefix => src.toUpperCase().startsWith(prefix)})
        CRS.fromWKT(src)
      else None
  }

  @transient
  private lazy val mapper: PartialFunction[String, CRS] = {
    case e if e.toUpperCase().startsWith("EPSG")   => CRS.fromName(e) //not case-sensitive
    case p if p.startsWith("+proj")                => CRS.fromString(p) // case sensitive
    case WKTCRS(w) => w
  }

  @transient
  private lazy val cache = Scaffeine().build[String, CRS](mapper)

  def apply(crs: CRS): LazyCRS = apply(crs.toProj4String)

  def apply(value: String): LazyCRS = {
    if (mapper.isDefinedAt(value)) {
      new LazyCRS(value.asInstanceOf[EncodedCRS])
    }
    else throw new IllegalArgumentException(
      s"CRS string must be either EPSG code, +proj string, or OGC WKT (WKT1). Argument value was ${if (value.length > 50) value.substring(0, 50) + "..." else value} ")
  }
}
