package org.locationtech.rasterframes.encoders

import frameless._
import geotrellis.layer.{KeyBounds, LayoutDefinition, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.focal.{Kernel, Neighborhood}
import geotrellis.raster.{CellGrid, CellType, Dimensions, GridBounds, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.rf.{CrsUDT, RasterSourceUDT, TileUDT}
import org.locationtech.jts.geom.Envelope
import org.locationtech.rasterframes.util.{FocalNeighborhood, KryoSupport}

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

  implicit val neighborhoodInjection: Injection[Neighborhood, String] = Injection(FocalNeighborhood(_), FocalNeighborhood.fromString(_).get)
  implicit val neighborhoodTypedEncoder: TypedEncoder[Neighborhood]   = TypedEncoder.usingInjection

  implicit val envelopeTypedEncoder: TypedEncoder[Envelope] =
    ManualTypedEncoder.newInstance[Envelope](
      fields = List(
        RecordEncoderField(0, "minX", TypedEncoder[Double]),
        RecordEncoderField(1, "maxX", TypedEncoder[Double]),
        RecordEncoderField(2, "minY", TypedEncoder[Double]),
        RecordEncoderField(3, "maxY", TypedEncoder[Double])
      ),
      fieldNameModify = { fieldName => s"get${fieldName.capitalize}" }
    )

  implicit def dimensionsTypedEncoder[N: Integral: TypedEncoder]: TypedEncoder[Dimensions[N]] =
    ManualTypedEncoder.staticInvoke[Dimensions[N]](
      fields = List(
        RecordEncoderField(0, "cols", TypedEncoder[N]),
        RecordEncoderField(1, "rows", TypedEncoder[N])
      )
    )

  /**
   * @note
   * Frameless cannot derive encoder for GridBounds because it lacks constructor from (int, int, int int)
   * Defining Injection is not suitable because Injection is used in derivation of encoder fields but is not an encoder.
   * Additionally Injection to Tuple4[Int, Int, Int, Int] would not have correct fields.
   */
  implicit def gridBoundsTypedEncoder[N: Integral: TypedEncoder]: TypedEncoder[GridBounds[N]] =
    ManualTypedEncoder.staticInvoke[GridBounds[N]](
      fields = List(
        RecordEncoderField(0, "colMin", TypedEncoder[N]),
        RecordEncoderField(1, "rowMin", TypedEncoder[N]),
        RecordEncoderField(2, "colMax", TypedEncoder[N]),
        RecordEncoderField(3, "rowMax", TypedEncoder[N])
      )
    )

  implicit def tileLayerMetadataTypedEncoder[K: TypedEncoder: ClassTag]: TypedEncoder[TileLayerMetadata[K]] =
    ManualTypedEncoder.staticInvoke[TileLayerMetadata[K]](
      fields = List(
        RecordEncoderField(0, "cellType", TypedEncoder[CellType]),
        RecordEncoderField(1, "layout", TypedEncoder[LayoutDefinition]),
        RecordEncoderField(2, "extent", TypedEncoder[Extent]),
        RecordEncoderField(3, "crs", TypedEncoder[CRS]),
        RecordEncoderField(4, "bounds", TypedEncoder[KeyBounds[K]])
      )
    )

  implicit val tileTypedEncoder: TypedEncoder[Tile] = TypedEncoder.usingUserDefinedType[Tile]
  implicit def rasterTileTypedEncoder[T <: CellGrid[Int]: TypedEncoder]: TypedEncoder[Raster[T]] = TypedEncoder.usingDerivation

  implicit val kernelTypedEncoder: TypedEncoder[Kernel] = TypedEncoder.usingDerivation
}

object TypedEncoders extends TypedEncoders
