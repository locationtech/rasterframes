package org.locationtech.rasterframes.datasource.stac.api

import cats.syntax.either._
import io.circe.{Decoder, Json}

package object encoders extends StacSerializers {
  implicit class JsonOps(val json: Json) extends AnyVal {
    def asUnsafe[T: Decoder]: T = json.as[T].valueOr(throw _)
  }
}
