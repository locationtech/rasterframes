package org.locationtech.rasterframes.encoders

import frameless._
import geotrellis.layer.{KeyBounds, LayoutDefinition, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Dimensions, GridBounds, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.rf.{CrsUDT, RasterSourceUDT, TileUDT}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.locationtech.jts.geom.Envelope
import org.locationtech.rasterframes.util.KryoSupport

import java.net.URI
import java.nio.ByteBuffer
import scala.reflect.ClassTag

trait TypedEncoders {
  def typedExpressionEncoder[T: TypedEncoder]: ExpressionEncoder[T] = TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  implicit val crsUDT = new CrsUDT
  implicit val tileUDT = new TileUDT
  implicit val rasterSourceUDT = new RasterSourceUDT

  implicit val cellTypeInjection: Injection[CellType, String] = Injection(_.toString, CellType.fromName)
  implicit val cellTypeTypedEncoder: TypedEncoder[CellType] = TypedEncoder.usingInjection[CellType, String]

  implicit val quantileSummariesInjection: Injection[QuantileSummaries, Array[Byte]] =
    Injection(KryoSupport.serialize(_).array(), array => KryoSupport.deserialize[QuantileSummaries](ByteBuffer.wrap(array)))

  implicit val quantileSummariesTypedEncoder: TypedEncoder[QuantileSummaries] = TypedEncoder.usingInjection

  implicit val uriInjection: Injection[URI, String] = Injection(_.toString, new URI(_))
  implicit val uriTypedEncoder: TypedEncoder[URI]   = TypedEncoder.usingInjection

  implicit val envelopeTypedEncoder: TypedEncoder[Envelope] = new TypedEncoder[Envelope] {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "minX", TypedEncoder[Double]),
      RecordEncoderField(1, "maxX", TypedEncoder[Double]),
      RecordEncoderField(2, "minY", TypedEncoder[Double]),
      RecordEncoderField(3, "maxY", TypedEncoder[Double])
    )

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[Envelope]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      // val newExpr = StaticInvoke(EnvelopeLocal.getClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, s"get${field.name.capitalize}", field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  implicit val dimensionsTypedEncoder: TypedEncoder[Dimensions[Int]] = new TypedEncoder[Dimensions[Int]] {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "cols", TypedEncoder[Int]),
      RecordEncoderField(1, "rows", TypedEncoder[Int]))

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[Dimensions[Int]]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      //val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      val newExpr = StaticInvoke(DimensionsInt.getClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  /**
   * @note
   * Frameless cannot derive encoder for GridBounds because it lacks constructor from (int, int, int int)
   * Defining Injection is not suitable because Injection is used in derivation of encoder fields but is not an encoder.
   * Additionally Injection to Tuple4[Int, Int, Int, Int] would not have correct fields.
   */
  implicit val gridBoundsTypedEncoder: TypedEncoder[GridBounds[Int]] = new TypedEncoder[GridBounds[Int]]() {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "colMin", TypedEncoder[Int]),
      RecordEncoderField(1, "rowMin", TypedEncoder[Int]),
      RecordEncoderField(2, "colMax", TypedEncoder[Int]),
      RecordEncoderField(3, "rowMax", TypedEncoder[Int]))

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[GridBounds[Int]]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      //val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      val newExpr = StaticInvoke(classTag.runtimeClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  implicit def tileLayerMetadataTypedEncoder[K: TypedEncoder: ClassTag]: TypedEncoder[TileLayerMetadata[K]] = new TypedEncoder[TileLayerMetadata[K]] {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "cellType", cellTypeTypedEncoder),
      RecordEncoderField(1, "layout", TypedEncoder[LayoutDefinition]),
      RecordEncoderField(2, "extent", TypedEncoder[Extent]),
      RecordEncoderField(3, "crs", TypedEncoder[CRS]),
      RecordEncoderField(4, "bounds", TypedEncoder[KeyBounds[K]])
    )

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[TileLayerMetadata[K]]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      // val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      val newExpr = StaticInvoke(classTag.runtimeClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  implicit val tileTypedEncoder: TypedEncoder[Tile] = TypedEncoder.usingUserDefinedType[Tile]
  implicit val rasterTileTypedEncoder: TypedEncoder[Raster[Tile]] = TypedEncoder.usingDerivation

}

object TypedEncoders extends TypedEncoders