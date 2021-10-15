package org.locationtech.rasterframes.datasource.stac.api

import org.locationtech.rasterframes.datasource.stac.api.encoders._

import com.azavea.stac4s.api.client.SearchFilters
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import sttp.model.Uri

class StacApiScanBuilder(uri: Uri, searchFilters: SearchFilters) extends ScanBuilder {
  def build(): Scan = new StacApiBatchScan(uri, searchFilters)
}

/** Batch Reading Support. The schema is repeated here as it can change after column pruning, etc. */
class StacApiBatchScan(uri: Uri, searchFilters: SearchFilters) extends Scan with Batch {
  def readSchema(): StructType = stacItemEncoder.schema

  override def toBatch: Batch = this

  /**
   * Unfortunately, we can only load everything into a single partition, due to the nature of STAC API endpoints.
   * To perform a distributed load, we'd need to know some internals about how the next page token is computed.
   * This can be a good idea for the STAC Spec extension.
   * */
  def planInputPartitions(): Array[InputPartition] = Array(StacApiPartition(uri, searchFilters))
  def createReaderFactory(): PartitionReaderFactory = new StacApiPartitionReaderFactory()
}
