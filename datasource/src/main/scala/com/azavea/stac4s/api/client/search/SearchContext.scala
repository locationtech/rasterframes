package com.azavea.stac4s.api.client.search

import io.circe.generic.JsonCodec

@JsonCodec
case class SearchContext(returned: Int, matched: Int)
