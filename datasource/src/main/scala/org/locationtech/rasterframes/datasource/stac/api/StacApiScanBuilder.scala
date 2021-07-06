package org.locationtech.rasterframes.datasource.stac.api

import com.azavea.stac4s.StacItem
import com.azavea.stac4s.api.client.SearchFilters
import eu.timepit.refined.types.numeric.NonNegInt
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import sttp.model.Uri

class StacApiScanBuilder(uri: Uri, searchFilters: SearchFilters, searchLimit: Option[NonNegInt])
                        (implicit val stacItemEncoder: ExpressionEncoder[StacItem]) extends ScanBuilder {
  override def build(): Scan = new StacApiBatchScan(uri, searchFilters, searchLimit)
}

/** Batch Reading Support. The schema is repeated here as it can change after column pruning, etc. */
class StacApiBatchScan(uri: Uri, searchFilters: SearchFilters, searchLimit: Option[NonNegInt])
                      (implicit val stacItemEncoder: ExpressionEncoder[StacItem]) extends Scan with Batch {
  def readSchema(): StructType = stacItemEncoder.schema

  override def toBatch: Batch = this

  /**
   * Unfortunately, we can only load everything into a single partition, due to the nature of STAC API endpoints.
   * To perform a distributed load, we'd need to know some internals about how the next page token is computed.
   * This can be a good idea for the STAC Spec extension.
   * */
  def planInputPartitions(): Array[InputPartition] = Array(StacApiPartition(uri, searchFilters, searchLimit))
  def createReaderFactory(): PartitionReaderFactory = new StacApiPartitionReaderFactory()
}
