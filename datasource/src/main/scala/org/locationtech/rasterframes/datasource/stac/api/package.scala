package org.locationtech.rasterframes.datasource.stac

import cats.Monad
import cats.syntax.functor._
import com.azavea.stac4s.api.client.SearchFilters
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import io.circe.syntax._
import fs2.{Pull, Stream}
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
    def loadStac(limit: Int): StacApiDataFrame = tag[StacApiDataFrameTag][DataFrame](reader.load.limit(limit))
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
    /** Unsafe API to interop with the Spark API. */
    def toIterator(run: F[Option[(T, fs2.Stream[F, T])]] => Option[(T, fs2.Stream[F, T])])
                  (implicit monad: Monad[F], compiler: Stream.Compiler[F, F]): Iterator[T] = new Iterator[T] {
      private var head = self
      private def nextF: F[Option[(T, fs2.Stream[F, T])]] =
        head
          .pull.uncons1
          .flatMap(Pull.output1)
          .stream
          .compile
          .last
          .map(_.flatten)

      def hasNext(): Boolean = run(nextF).nonEmpty

      def next(): T = {
        val (item, tail) = run(nextF).get
        this.head = tail
        item
      }
    }
  }

  implicit class DataFrameReaderOps(val self: DataFrameReader) extends AnyVal {
    def option(key: String, value: Option[String]): DataFrameReader = value.fold(self)(self.option(key, _))
    def option(key: String, value: Option[Int])(implicit d: DummyImplicit): DataFrameReader = value.fold(self)(self.option(key, _))
  }

  implicit class DataFrameReaderStacApiOps(val reader: DataFrameReader) extends AnyVal {
    def stacApi(): StacApiDataFrameReader = tag[StacApiDataFrameTag][DataFrameReader](reader.format(StacApiDataSource.SHORT_NAME))
    def stacApi(uri: String, filters: SearchFilters = SearchFilters()): StacApiDataFrameReader =
      tag[StacApiDataFrameTag][DataFrameReader](
        stacApi()
          .option(StacApiDataSource.URI_PARAM, uri)
          .option(StacApiDataSource.SEARCH_FILTERS_PARAM, filters.asJson.noSpaces)
      )
  }
}
