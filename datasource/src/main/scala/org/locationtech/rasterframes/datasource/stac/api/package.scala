package org.locationtech.rasterframes.datasource.stac

import com.azavea.stac4s.api.client.SearchFilters
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import io.circe.syntax._
import fs2.Stream
import shapeless.tag
import shapeless.tag.@@
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

package object api {
  // TODO: replace TypeTags with newtypes?
  trait StacApiDataFrameTag
  type StacApiDataFrameReader = DataFrameReader @@ StacApiDataFrameTag
  type StacApiDataFrame = DataFrame @@ StacApiDataFrameTag

  implicit class StacApiDataFrameReaderOps(val reader: StacApiDataFrameReader) extends AnyVal {
    def loadStac: StacApiDataFrame = tag[StacApiDataFrameTag][DataFrame](reader.load)
  }

  implicit class StacApiDataFrameOps(val df: StacApiDataFrame) extends AnyVal {
    // TODO: add more overloads, by the asset type?
    def flattenAssets(implicit spark: SparkSession): StacApiDataFrame = {
      import spark.implicits._
      tag[StacApiDataFrameTag][DataFrame](
        df
          .select(
            df.columns.map {
              case "assets" => explode($"assets")
              case s        => $"$s"
            }: _*
          )
          .withColumnRenamed("key", "assetName")
          .withColumnRenamed("value", "asset")
      )
    }
  }

  implicit class Fs2StreamOps[F[_], T](val self: Stream[F, T]) {
    def take(n: Option[Int]): Stream[F, T] = n.fold(self)(self.take(_))
  }

  implicit class DataFrameReaderOps(val self: DataFrameReader) extends AnyVal {
    def option(key: String, value: Option[String]): DataFrameReader = value.fold(self)(self.option(key, _))
    def option(key: String, value: Option[Int])(implicit d: DummyImplicit): DataFrameReader = value.fold(self)(self.option(key, _))
  }

  implicit class DataFrameReaderStacApiOps(val reader: DataFrameReader) extends AnyVal {
    def stacApi(): StacApiDataFrameReader = tag[StacApiDataFrameTag][DataFrameReader](reader.format(StacApiDataSource.SHORT_NAME))
    def stacApi(uri: String, filters: SearchFilters = SearchFilters(), searchLimit: Option[Int] = None): StacApiDataFrameReader =
      tag[StacApiDataFrameTag][DataFrameReader](
        stacApi()
          .option(StacApiDataSource.URI_PARAM, uri)
          .option(StacApiDataSource.SEARCH_FILTERS_PARAM, filters.asJson.noSpaces)
          .option(StacApiDataSource.SEARCH_LIMIT_PARAM, searchLimit)
      )
  }
}
