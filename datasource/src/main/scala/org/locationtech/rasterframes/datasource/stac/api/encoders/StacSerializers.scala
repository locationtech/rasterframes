package org.locationtech.rasterframes.datasource.stac.api.encoders

import io.circe.parser.parse
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import cats.syntax.either._
import com.azavea.stac4s._
import com.azavea.stac4s.types.ItemDatetime
import eu.timepit.refined.api.{RefType, Validate}
import frameless.{Injection, SQLTimestamp, TypedEncoder, TypedExpressionEncoder}
import frameless.refined
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.jts.JTSTypes

import java.time.Instant
import scala.reflect.ClassTag

/** STAC API Dataframe relies on the Frameless Expressions derivation. */
trait StacSerializers {
  /** GeoMesa UDTs, should be defined as implicits so frameless would pick them up */
  implicit val pointUDT = JTSTypes.PointTypeInstance
  implicit val multiPointUDT = JTSTypes.MultiPointTypeInstance
  implicit val multiLineStringUDT = JTSTypes.MultiLineStringTypeInstance
  implicit val polygonUDT = JTSTypes.PolygonTypeInstance
  implicit val multiPolygonUDT = JTSTypes.MultipolygonTypeInstance
  implicit val geometryUDT = JTSTypes.GeometryTypeInstance
  implicit val geometryCollectionUDT = JTSTypes.GeometryCollectionTypeInstance

  /** Injections to Encode stac4s objects */
  implicit val stacLinkTypeInjection: Injection[StacLinkType, String] = Injection(_.repr, _.asJson.asUnsafe[StacLinkType])
  implicit val stacMediaTypeInjection: Injection[StacMediaType, String] = Injection(_.repr, _.asJson.asUnsafe[StacMediaType])
  implicit val stacAssetRoleInjection: Injection[StacAssetRole, String] = Injection(_.repr, _.asJson.asUnsafe[StacAssetRole])
  implicit val stacLicenseInjection: Injection[StacLicense, String] = Injection(_.name, _.asJson.asUnsafe[StacLicense])
  implicit val stacProviderRoleInjection: Injection[StacProviderRole, String] = Injection(_.repr, _.asJson.asUnsafe[StacProviderRole])

  /** Injections to Encode circe objects */
  implicit val jsonInjection: Injection[Json, String] = Injection(_.noSpaces, parse(_).valueOr(throw _))
  implicit val jsonObjectInjection: Injection[JsonObject, String] = Injection(_.asJson.noSpaces, parse(_).flatMap(_.as[JsonObject]).valueOr(throw _))

  /** Injection to support [[java.time.Instant]] */
  implicit val instantInjection: Injection[Instant, SQLTimestamp] = Injection(i => SQLTimestamp(i.toEpochMilli), s => Instant.ofEpochMilli(s.us))

  /** ItemDatetime should have a separate catalyst representation */
  implicit val itemDatetimeCatalystType: Injection[ItemDatetimeCatalystType, String] = Injection(_.repr, ItemDatetimeCatalystType.fromString)
  implicit val itemDatetimeInjection: Injection[ItemDatetime, ItemDatetimeCatalyst] = Injection(ItemDatetimeCatalyst.fromItemDatetime, ItemDatetimeCatalyst.toDatetime)

  /** Refined types support, proxies to avoid frameless.refined import in the client code */
  implicit def refinedInjection[F[_, _]: RefType, T, R: Validate[T, *]]: Injection[F[T, R], T] =
    refined.refinedInjection

  implicit def refinedEncoder[F[_, _]: RefType, T: TypedEncoder, R: Validate[T, *]](implicit ct: ClassTag[F[T, R]]): TypedEncoder[F[T, R]] =
    refined.refinedEncoder

  /** Set would be stored as Array */
  implicit def setInjection[T]: Injection[Set[T], List[T]] = Injection(_.toList, _.toSet)

  /** TypedExpressionEncoder upcasts ExpressionEncoder up to Encoder, we need an ExpressionEncoder there */
  def typedToExpressionEncoder[T: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  /** High priority specific product encoder derivation. Without it, the default spark would be used. */
  implicit def productTypedToExpressionEncoder[T <: Product: TypedEncoder]: ExpressionEncoder[T] = typedToExpressionEncoder

  implicit val stacItemEncoder: ExpressionEncoder[StacItem] = typedToExpressionEncoder[StacItem]
}
