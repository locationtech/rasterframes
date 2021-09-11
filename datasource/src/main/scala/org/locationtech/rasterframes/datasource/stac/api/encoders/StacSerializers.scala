package org.locationtech.rasterframes.datasource.stac.api.encoders

import org.locationtech.rasterframes.datasource.stac.api.encoders.syntax._

import io.circe.parser.parse
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import cats.syntax.either._
import com.azavea.stac4s._
import eu.timepit.refined.api.{RefType, Validate}
import frameless.{Injection, SQLTimestamp, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.stac._

import java.time.Instant

/** STAC API Dataframe relies on the Frameless Expressions derivation. */
trait StacSerializers {
  /** GeoMesa UDTs, should be defined as implicits so frameless would pick them up */
  implicit val pointUDT: PointUDT = new PointUDT
  implicit val multiPointUDT: MultiPointUDT = new MultiPointUDT
  implicit val multiLineStringUDT: MultiLineStringUDT = new MultiLineStringUDT
  implicit val polygonUDT: PolygonUDT = new PolygonUDT
  implicit val multiPolygonUDT: MultiPolygonUDT = new MultiPolygonUDT
  implicit val geometryUDT: GeometryUDT = new GeometryUDT
  implicit val geometryCollectionUDT: GeometryCollectionUDT = new GeometryCollectionUDT

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

  /** Refined types support, https://github.com/typelevel/frameless/issues/257#issuecomment-914392485 */
  implicit def refinedInjection[F[_, _], T, P](implicit refType: RefType[F], validate: Validate[T, P]): Injection[F[T, P], T] =
    Injection(refType.unwrap, value => refType.refine[P](value).valueOr(errMsg => throw new IllegalArgumentException(s"Value $value does not satisfy refinement predicate: $errMsg")))

  /** Set would be stored as Array */
  implicit def setInjection[T]: Injection[Set[T], List[T]] = Injection(_.toList, _.toSet)

  /** TypedExpressionEncoder upcasts ExpressionEncoder up to Encoder, we need an ExpressionEncoder there */
  def typedToExpressionEncoder[T: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  /** High priority specific product encoder derivation. Without it, the default spark would be used. */
  implicit def productTypedToExpressionEncoder[T <: Product: TypedEncoder]: ExpressionEncoder[T] = typedToExpressionEncoder
}
