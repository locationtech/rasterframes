package org.locationtech.rasterframes.datasource.stac.api

import cats.effect.IO
import com.azavea.stac4s.StacItem
import geotrellis.store.util.BlockingThreadPool
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import com.azavea.stac4s.api.client._
import eu.timepit.refined.types.numeric.NonNegInt
import sttp.model.Uri

case class StacApiPartition(uri: Uri, searchFilters: SearchFilters, searchLimit: Option[NonNegInt]) extends InputPartition

class StacApiPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: StacApiPartition => new StacApiPartitionReader(p)
      case _                   => throw new UnsupportedOperationException("Partition processing is unsupported by the reader.")
    }
  }
}

class StacApiPartitionReader(partition: StacApiPartition) extends PartitionReader[InternalRow] {

  lazy val partitionValues: Iterator[StacItem] = {
    implicit val cs = IO.contextShift(BlockingThreadPool.executionContext)
    AsyncHttpClientCatsBackend
      .resource[IO]()
      .use { backend =>
        val stream = SttpStacClient(backend, partition.uri).search(partition.searchFilters)
        partition
          .searchLimit
          .fold(stream)(n => stream.take(n.value))
          .compile
          .toList
      }
      .map(_.toIterator)
      .unsafeRunSync()
  }

  def next: Boolean = partitionValues.hasNext

  def get: InternalRow = {
    val partitionValue = partitionValues.next
    val stringUtf = UTF8String.fromString(partitionValue.toString)
    InternalRow(stringUtf)
  }

  def close(): Unit = { }
}
