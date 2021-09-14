package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

import scala.collection.concurrent.TrieMap

import scala.reflect.runtime.universe.TypeTag

object SerializersCache { self =>
  /**
   * The point of {Serizalizer | Deserializer} wrappers to make application atomic.
   * If that is the chain of encoders, i.e. T <=> InternalRow <=> Row the whole chain applciation should be atomic.
   */
  case class DeserializerSynchronized[T](underlying: ExpressionEncoder.Deserializer[T]) {
    def apply(i: InternalRow): T = self.synchronized(underlying.apply(i))
  }

  case class SerializerSynchronized[T](underlying: ExpressionEncoder.Serializer[T]) {
    // copy should happen within the same lock, otherwise we're risking to loose the InternalRow
    def apply(t: T): InternalRow = self.synchronized(underlying.apply(t).copy())
  }

  case class DeserializerRowSynchronized[T](underlying: Row => T) extends AnyVal {
    def apply(i: Row): T = self.synchronized(underlying(i))
  }

  case class SerializerRowSynchronized[T](underlying: T => Row) extends AnyVal {
    def apply(i: T): Row = self.synchronized(underlying(i))
  }

  private val cacheSerializer: TrieMap[TypeTag[_], SerializerSynchronized[_]] = TrieMap.empty
  private val cacheSerializerRow: TrieMap[TypeTag[_], SerializerSynchronized[Row]] = TrieMap.empty
  private val cacheDeserializer: TrieMap[TypeTag[_], DeserializerSynchronized[_]] = TrieMap.empty
  private val cacheDeserializerRow: TrieMap[TypeTag[_], DeserializerSynchronized[Row]] = TrieMap.empty

  /** Serializer is threadsafe.*/
  def serializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): SerializerSynchronized[T] =
    cacheSerializer
      .getOrElseUpdate(tag, SerializerSynchronized(encoder.createSerializer()))
      .asInstanceOf[SerializerSynchronized[T]]

  def rowSerializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): SerializerSynchronized[Row] =
    cacheSerializerRow.getOrElseUpdate(tag, SerializerSynchronized(RowEncoder(encoder.schema).createSerializer()))

  /** Both Serializer and Deserializer are not thread safe, and expensive to derive.
   * Per partition instance would give us no performance regressions,
   * however would require a significant DynamicExtractors refactor. */
  def deserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): DeserializerSynchronized[T] =
    cacheDeserializer
      .getOrElseUpdate(tag, DeserializerSynchronized(encoder.resolveAndBind().createDeserializer()))
      .asInstanceOf[DeserializerSynchronized[T]]


  def rowDeserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): DeserializerSynchronized[Row] =
    cacheDeserializerRow
      .getOrElseUpdate(tag, DeserializerSynchronized(RowEncoder(encoder.schema).resolveAndBind().createDeserializer()))


  /**
   * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-RowEncoder.html
   * https://github.com/apache/spark/blob/93cec49212fe82816fcadf69f429cebaec60e058/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala#L75-L86
   */
  def rowDeserialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): DeserializerRowSynchronized[T] =
    DeserializerRowSynchronized { row => deserializer[T](tag, encoder)(rowSerializer[T](tag, encoder)(row)) }

  def rowSerialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): SerializerRowSynchronized[T] =
    SerializerRowSynchronized[T] ({ t => rowDeserializer[T](tag, encoder)(serializer[T](tag, encoder)(t)) })
}
