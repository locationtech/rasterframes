package org.locationtech.rasterframes.datasource.stac.api

import org.locationtech.rasterframes.datasource.stac.api.encoders.syntax._

import com.azavea.stac4s.StacItem
import geotrellis.store.util.BlockingThreadPool
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import com.azavea.stac4s.api.client._
import eu.timepit.refined.types.numeric.NonNegInt
import cats.effect.IO
import sttp.model.Uri
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

case class StacApiPartition(uri: Uri, searchFilters: SearchFilters, searchLimit: Option[NonNegInt]) extends InputPartition

class StacApiPartitionReaderFactory(implicit val stacItemEncoder: ExpressionEncoder[StacItem]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: StacApiPartition => new StacApiPartitionReader(p)
      case _                   => throw new UnsupportedOperationException("Partition processing is unsupported by the reader.")
    }
  }
}

class StacApiPartitionReader(partition: StacApiPartition)(implicit val stacItemEncoder: ExpressionEncoder[StacItem]) extends PartitionReader[InternalRow] {
  lazy val partitionValues: Iterator[StacItem] = {
    implicit val cs = IO.contextShift(BlockingThreadPool.executionContext)
    AsyncHttpClientCatsBackend
      .resource[IO]()
      .use { backend =>
        SttpStacClient(backend, partition.uri)
          .search(partition.searchFilters)
          .take(partition.searchLimit.map(_.value))
          .compile
          .toList
      }
      .map(_.toIterator)
      .unsafeRunSync()
  }

  def next: Boolean = partitionValues.hasNext

  def get: InternalRow = partitionValues.next.toInternalRow

  def close(): Unit = { }
}
