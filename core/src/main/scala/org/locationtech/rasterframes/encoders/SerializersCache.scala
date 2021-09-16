package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object SerializersCache {
  /**
   * Spark partitions are executed on a blocking thread pool.
   * We can keep the cache of (De)Serializers (every serializer instance creation is pretty expensive),
   * but the cache should be local per thread.
   *
   * When used from multiple threads (De)Serializers tend to corrupt data and / or fail at runtime.
   * The alternative can be to use global locks or to use a separate executor per each (De)Serializer.
   */
  private class ThreadLocalHashMap[K, V] extends ThreadLocal[mutable.HashMap[K, V]] {
    override def initialValue(): mutable.HashMap[K, V] = mutable.HashMap.empty
  }
  private object ThreadLocalHashMap {
    def empty[K, V]: ThreadLocalHashMap[K, V] = new ThreadLocalHashMap
  }

  /** SerializerSafe ensures that all Serializers from the pool call copy after application. */
  case class SerializerSafe[T](underlying: ExpressionEncoder.Serializer[T]) {
    def apply(t: T): InternalRow = underlying.apply(t).copy()
  }

  // T => InternalRow
  private val cacheSerializer: ThreadLocalHashMap[TypeTag[_], SerializerSafe[_]] = ThreadLocalHashMap.empty
  // Row with Schema T => InternalRow
  private val cacheSerializerRow: ThreadLocalHashMap[TypeTag[_], SerializerSafe[Row]] = ThreadLocalHashMap.empty
  // InternalRow => T
  private val cacheDeserializer: ThreadLocalHashMap[TypeTag[_], ExpressionEncoder.Deserializer[_]] = ThreadLocalHashMap.empty
  // InternalRow => Row with Schema T
  private val cacheDeserializerRow: ThreadLocalHashMap[TypeTag[_], ExpressionEncoder.Deserializer[Row]] = ThreadLocalHashMap.empty

  def serializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): SerializerSafe[T] =
    cacheSerializer.get.getOrElseUpdate(tag, SerializerSafe(encoder.createSerializer())).asInstanceOf[SerializerSafe[T]]

  def rowSerializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): SerializerSafe[Row] =
    cacheSerializerRow.get.getOrElseUpdate(tag, SerializerSafe(RowEncoder(encoder.schema).createSerializer()))

  def deserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): ExpressionEncoder.Deserializer[T] =
    cacheDeserializer.get.getOrElseUpdate(tag, encoder.resolveAndBind().createDeserializer()).asInstanceOf[ExpressionEncoder.Deserializer[T]]

  def rowDeserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): ExpressionEncoder.Deserializer[Row] =
    cacheDeserializerRow.get.getOrElseUpdate(tag, RowEncoder(encoder.schema).resolveAndBind().createDeserializer())

  /**
   * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-RowEncoder.html
   * https://github.com/apache/spark/blob/93cec49212fe82816fcadf69f429cebaec60e058/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala#L75-L86
   */
  def rowDeserialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): Row => T =
    { row => deserializer[T](tag, encoder)(rowSerializer[T](tag, encoder)(row)) }

  def rowSerialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): T => Row =
    { t => rowDeserializer[T](tag, encoder)(serializer[T](tag, encoder)(t)) }
}
