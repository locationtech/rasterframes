package org.locationtech.rasterframes.datasource.stac.api

import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.datasource.stac.api.encoders._

import com.azavea.stac4s.StacItem
import geotrellis.store.util.BlockingThreadPool
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import com.azavea.stac4s.api.client._
import cats.effect.IO
import sttp.model.Uri
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

case class StacApiPartition(uri: Uri, searchFilters: SearchFilters) extends InputPartition

class StacApiPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: StacApiPartition => new StacApiPartitionReader(p)
      case _                   => throw new UnsupportedOperationException("Partition processing is unsupported by the reader.")
    }
  }
}

class StacApiPartitionReader(partition: StacApiPartition) extends PartitionReader[InternalRow] {

  @transient private implicit lazy val cs = IO.contextShift(BlockingThreadPool.executionContext)
  @transient private lazy val backend = AsyncHttpClientCatsBackend[IO]().unsafeRunSync()
  @transient private lazy val partitionValues: Iterator[StacItem] =
    SttpStacClient(backend, partition.uri)
      .search(partition.searchFilters)
      .toIterator(_.unsafeRunSync())

  def next: Boolean = partitionValues.hasNext

  def get: InternalRow = partitionValues.next.toInternalRow

  def close(): Unit = backend.close().unsafeRunSync()
}
