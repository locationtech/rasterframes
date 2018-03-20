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

package org.apache.spark.sql.gt.types

import org.apache.spark.sql.types._

import scala.reflect._

/**
 * Base class for several of the tile-related GeoTrellis UDTs.
 *
 * @author sfitch
 * @since 4/12/17
 */
private[gt] abstract class AbstractTileUDT[T >: Null: ClassTag](override val typeName: String)
    extends UserDefinedType[T]
    with KryoBackedUDT[T] {
  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case o: AbstractTileUDT[T] ⇒ o.typeName == this.typeName
    case _ ⇒ super.acceptsType(dataType)
  }

  override val targetClassTag = classTag[T]
}
