package org.locationtech.rasterframes.datasource.shapefile

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

import java.net.URL

class ShapeFileScanBuilder(url: URL) extends ScanBuilder {
  def build(): Scan = new ShapeFileBatchScan(url)
}

/** Batch Reading Support. The schema is repeated here as it can change after column pruning, etc. */
class ShapeFileBatchScan(url: URL) extends Scan with Batch {
  def readSchema(): StructType = geometryExpressionEncoder.schema

  override def toBatch: Batch = this

  /** Unfortunately, we can only load one file into a single partition only.*/
  def planInputPartitions(): Array[InputPartition] = Array(ShapeFilePartition(url))
  def createReaderFactory(): PartitionReaderFactory = new ShapeFilePartitionReaderFactory()
}
