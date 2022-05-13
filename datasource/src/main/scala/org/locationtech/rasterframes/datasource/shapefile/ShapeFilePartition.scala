package org.locationtech.rasterframes.datasource.shapefile

import org.locationtech.rasterframes.encoders.syntax._

import geotrellis.vector.Geometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureIterator

import java.net.URL

case class ShapeFilePartition(url: URL) extends InputPartition

class ShapeFilePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = partition match {
    case p: ShapeFilePartition => new ShapeFilePartitionReader(p)
    case _ => throw new UnsupportedOperationException("Partition processing is unsupported by the reader.")
  }
}

class ShapeFilePartitionReader(partition: ShapeFilePartition) extends PartitionReader[InternalRow] {
  import geotrellis.shapefile.ShapeFileReader._

  @transient lazy val ds = new ShapefileDataStore(partition.url)
  @transient lazy val partitionValues: SimpleFeatureIterator = ds.getFeatureSource.getFeatures.features

  def next: Boolean = partitionValues.hasNext

  def get: InternalRow = partitionValues.next.geom[Geometry].toInternalRow

  def close(): Unit = { partitionValues.close(); ds.dispose() }
}
