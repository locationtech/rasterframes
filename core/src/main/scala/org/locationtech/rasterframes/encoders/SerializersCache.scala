package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

import scala.collection.concurrent.TrieMap

import scala.reflect.runtime.universe.TypeTag

object SerializersCache { self =>
  /** The point of these wrappers to make application atomic.
   * If that is the chain of encoders, i.e. T <=> InternalRow <=> Row the whole chain should be atomic.
   */
  case class DeserializerCached[T](underlying: ExpressionEncoder.Deserializer[T]) {
    def apply(i: InternalRow): T = self.synchronized(underlying.apply(i))
  }

  case class RowDeserializerCached[T](underlying: Row => T) {
    def apply(i: Row): T = self.synchronized(underlying(i))
  }

  case class RowSerializerCached[T](underlying: T => Row) {
    def apply(i: T): Row = self.synchronized(underlying(i))
  }

  private val cacheSerializer: TrieMap[TypeTag[_], ExpressionEncoder.Serializer[_]] = TrieMap.empty
  private val cacheRowSerializer: TrieMap[TypeTag[_], ExpressionEncoder.Serializer[Row]] = TrieMap.empty
  private val cacheDeserializer: TrieMap[TypeTag[_], DeserializerCached[_]] = TrieMap.empty
  private val cacheRowDeserializer: TrieMap[TypeTag[_], DeserializerCached[Row]] = TrieMap.empty

  private val cacheRowDeserializerF: TrieMap[TypeTag[_], RowDeserializerCached[_]] = TrieMap.empty
  private val cacheRowSerializerF: TrieMap[TypeTag[_], RowSerializerCached[_]] = TrieMap.empty

  /** Serializer is threadsafe.*/
  def serializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): ExpressionEncoder.Serializer[T] =
    cacheSerializer
      .getOrElseUpdate(tag, encoder.createSerializer())
      .asInstanceOf[ExpressionEncoder.Serializer[T]]

  def rowSerializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): ExpressionEncoder.Serializer[Row] =
    cacheRowSerializer.getOrElseUpdate(tag, RowEncoder(encoder.schema).createSerializer())

  /** Deserializer is not thread safe, and expensive to derive.
   * Per partition instance would give us no performance regressions,
   * however would require a significant DynamicExtractors refactor. */
  def deserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): DeserializerCached[T] =
    cacheDeserializer
      .getOrElseUpdate(tag, DeserializerCached(encoder.resolveAndBind().createDeserializer()))
      .asInstanceOf[DeserializerCached[T]]

  def rowDeserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): DeserializerCached[Row] =
    cacheRowDeserializer.getOrElseUpdate(tag, DeserializerCached(RowEncoder(encoder.schema).resolveAndBind().createDeserializer()))

  /**
   * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-RowEncoder.html
   * https://github.com/apache/spark/blob/93cec49212fe82816fcadf69f429cebaec60e058/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala#L75-L86
   */
  def rowDeserialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): RowDeserializerCached[T] =
    cacheRowDeserializerF.getOrElseUpdate(tag, RowDeserializerCached { row =>
      deserializer[T](tag, encoder)(rowSerializer[T](tag, encoder)(row))
    }).asInstanceOf[RowDeserializerCached[T]]

  def rowSerialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): RowSerializerCached[T] =
    cacheRowSerializerF.getOrElseUpdate(tag, RowSerializerCached[T] ({ t =>
      rowDeserializer[T](tag, encoder)(serializer[T](tag, encoder)(t))
    })).asInstanceOf[RowSerializerCached[T]]
}
