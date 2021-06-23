package com.azavea.stac4s.api.client

import cats.{ApplicativeThrow, Monad}
import cats.syntax.flatMap._
import cats.syntax.either._
import com.azavea.stac4s.StacItem
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import sttp.client3.circe.asJson
import sttp.client3.basicRequest
import fs2.Stream

package object search {
  implicit class Stac4sClientOps[F[_]: Monad: ApplicativeThrow](val self: SttpStacClient[F]) {
    def search(filter: Option[SearchFilters]): Stream[F, StacItem] = filter.fold(self.search)(self.search)

    def searchContext(filter: Option[SearchFilters]): F[SearchContext] =
      self
        .client
        .send(
          basicRequest
            .body(filter.map(_.asJson).getOrElse(JsonObject.empty.asJson).noSpaces)
            .post(self.baseUri.addPath("search"))
            .response(asJson[Json])
        )
        .flatMap {
          _
            .body
            .flatMap(_.hcursor.downField("context").as[SearchContext]).liftTo[F]
        }
  }
}
