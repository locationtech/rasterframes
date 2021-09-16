package com.azavea.stac4s.api.client

import com.azavea.stac4s.StacItem
import fs2.Stream

package object search {
  implicit class Stac4sClientOps[F[_]](val self: SttpStacClient[F]) extends AnyVal {
    def search(filter: Option[SearchFilters]): Stream[F, StacItem] = filter.fold(self.search)(self.search)
  }
}
