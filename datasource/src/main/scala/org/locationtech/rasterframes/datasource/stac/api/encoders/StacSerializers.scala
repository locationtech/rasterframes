package org.locationtech.rasterframes.datasource.stac.api.encoders

import org.locationtech.rasterframes.encoders.CatalystSerializerEncoder
import com.azavea.stac4s.{ItemDatetime, ItemProperties, StacAsset, StacAssetRole, StacItem, StacLicense, StacLink, StacLinkType, StacMediaType, StacProvider, StacProviderRole, TwoDimBbox}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer.{CatalystIO, schemaOf}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.WKTReader
import com.azavea.stac4s.ItemDatetime.{PointInTime, TimeRange}
import eu.timepit.refined.types.string.NonEmptyString
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import cats.syntax.option._
import cats.data.NonEmptyList
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import java.time.Instant
import scala.util.Try

trait StacSerializers {

  implicit class StringOps(val self: String) {
    def toUTF8String: UTF8String = UTF8String.fromString(self)
  }

  implicit class MapStringJsonOps(val self: Map[String, Json]) {
    def toUTF8String: Map[UTF8String, UTF8String] = self.map { case (k, v) => k.toUTF8String -> v.spaces2.toUTF8String }
    def toMapData: MapData = ArrayBasedMapData(toUTF8String)
  }

  implicit class MapAnyAnyOps[K, V](val self: Map[K, V]) {
    def toMapStringJson: Map[String, Json] = self.map { case (k, v) => k.asInstanceOf[UTF8String].toString -> v.asInstanceOf[UTF8String].toString.asJson }
    def toJsonObject: JsonObject = JsonObject.fromMap(toMapStringJson)
  }

  implicit class ListOps[T](val self: List[T]) {
    def toArrayData: ArrayData = ArrayData.toArrayData(self)
  }

  implicit class CatalystIOOps[R](val self: CatalystIO[R]) {
    def getDoubleOption(d: R, ordinal: Int): Option[Double] = if(self.isNullAt(d, ordinal)) None else self.getDouble(d, ordinal).some
    def getStringOption(d: R, ordinal: Int): Option[String] = if(self.isNullAt(d, ordinal)) None else self.getString(d, ordinal).some
    def getArrayOption[T >: Null](d: R, ordinal: Int): Option[Array[T]] = if(self.isNullAt(d, ordinal)) None else self.getArray[T](d, ordinal).some
    def getMapOption[K >: Null, V >: Null](d: R, ordinal: Int): Option[Map[K, V]] = if(self.isNullAt(d, ordinal)) None else self.getMap[K, V](d, ordinal).some
    def getInstantOption(d: R, ordinal: Int): Option[Instant] = if(self.isNullAt(d, ordinal)) None else self.getInstant(d, ordinal).some
    def getNonEmptyString(d: R, ordinal: Int): Option[NonEmptyString] =
      self.getStringOption(d, ordinal).flatMap(NonEmptyString.from(_).toOption)

    def getNonEmptyListNonEmptyString(d: R, ordinal: Int): Option[NonEmptyList[NonEmptyString]] =
      getArrayOption[String](d, ordinal).map(_.flatMap(NonEmptyString.from(_).toOption).toList).flatMap(NonEmptyList.fromList)
  }


  implicit val geometrySerializer: CatalystSerializer[Geometry] = new CatalystSerializer[Geometry] {
    def schema: StructType = StructType(Seq(StructField("wkt", StringType, false)))

    protected def to[R](t: Geometry, io: CatalystIO[R]): R = io.create(t.toText.toUTF8String)

    protected def from[R](t: R, io: CatalystIO[R]): Geometry = {
      val reader = new WKTReader()
      reader.read(io.getString(t, 0))
    }
  }

  implicit val bboxSerializer: CatalystSerializer[TwoDimBbox] = new CatalystSerializer[TwoDimBbox] {
    def schema: StructType = StructType(Seq(
      StructField("xmin", DoubleType, false),
      StructField("ymin", DoubleType, false),
      StructField("xmax", DoubleType, false),
      StructField("ymax", DoubleType, false)
    ))

    protected def to[R](t: TwoDimBbox, io: CatalystIO[R]): R = io.create(t.xmin, t.ymin, t.xmax, t.ymax)

    protected def from[R](t: R, io: CatalystIO[R]): TwoDimBbox = {
     TwoDimBbox(
       io.getDouble(t, 0),
       io.getDouble(t, 1),
       io.getDouble(t, 2),
       io.getDouble(t, 3)
     )
    }
  }

  implicit val stacLinkSerializer: CatalystSerializer[StacLink] = new CatalystSerializer[StacLink] {
    def schema: StructType = StructType(Seq(
      StructField("href", StringType, false),
      StructField("rel", StringType, false),
      StructField("_type", StringType, true),
      StructField("title", StringType, true),
      StructField("extensionFields", MapType(StringType, StringType), true)
    ))

    protected def to[R](t: StacLink, io: CatalystIO[R]): R = io.create(
      t.href.toUTF8String,
      t.rel.repr.toUTF8String,
      t._type.map(_.repr.toUTF8String).orNull,
      t.title.map(_.toUTF8String).orNull,
      t.extensionFields.toMap.toMapData
    )

    protected def from[R](t: R, io: CatalystIO[R]): StacLink = StacLink(
      href = io.getString(t, 0),
      rel = io.getStringOption(t, 1).flatMap(_.asJson.as[StacLinkType].toOption).get,
      _type = io.getStringOption(t, 2).flatMap(_.asJson.as[StacMediaType].toOption),
      title = io.getStringOption(t, 3),
      extensionFields = io.getMapOption[UTF8String, UTF8String](t, 4).map(_.toJsonObject).getOrElse(JsonObject.empty)
    )
  }

  implicit val stacAssetSerializer: CatalystSerializer[StacAsset] = new CatalystSerializer[StacAsset] {
    def schema: StructType = StructType(Seq(
      StructField("href", StringType, false),
      StructField("title", StringType, true),
      StructField("description", StringType, true),
      StructField("roles", ArrayType(StringType), false),
      StructField("_type", StringType, true),
      StructField("extensionFields", MapType(StringType, StringType), true)
    ))

    protected def to[R](t: StacAsset, io: CatalystIO[R]): R = io.create(
      t.href.toUTF8String,
      t.title.map(_.toUTF8String).orNull,
      t.description.map(_.toUTF8String).orNull,
      t.roles.toList.map(_.repr.toUTF8String).toArrayData,
      t._type.map(_.repr.toUTF8String).orNull,
      t.extensionFields.toMap.toMapData
    )

    protected def from[R](t: R, io: CatalystIO[R]): StacAsset = StacAsset(
      href = io.getString(t, 0),
      title = io.getStringOption(t, 1),
      description = io.getStringOption(t, 2),
      roles = io.getArray[UTF8String](t, 3).flatMap(_.toString.asJson.as[StacAssetRole].toOption).toSet,
      _type = io.getStringOption(t, 4).flatMap(_.asJson.as[StacMediaType].toOption),
      extensionFields = io.getMapOption[UTF8String, UTF8String](t, 5).map(_.toJsonObject).getOrElse(JsonObject.empty)
    )
  }

  implicit val pointInTimeSerializer: CatalystSerializer[PointInTime] = new CatalystSerializer[PointInTime] {
    def schema: StructType = StructType(Seq(StructField("when", TimestampType, false)))

    protected def to[R](t: PointInTime, io: CatalystIO[R]): R = io.create(t.when.toEpochMilli)

    protected def from[R](t: R, io: CatalystIO[R]): PointInTime = PointInTime(io.getInstant(t, 0))
  }

  implicit val timeRangeSerializer: CatalystSerializer[TimeRange] = new CatalystSerializer[TimeRange] {
    def schema: StructType = StructType(Seq(
      StructField("start", TimestampType, false),
      StructField("end", TimestampType, false)
    ))

    protected def to[R](t: TimeRange, io: CatalystIO[R]): R = io.create(t.start.toEpochMilli, t.end.toEpochMilli)

    protected def from[R](t: R, io: CatalystIO[R]): TimeRange = TimeRange(io.getInstant(t, 0), io.getInstant(t, 1))
  }

  implicit val itemDatetimeSerializer: CatalystSerializer[ItemDatetime] = new CatalystSerializer[ItemDatetime] {
    def schema: StructType = StructType(Seq(
      StructField("pointInTime", schemaOf[PointInTime], true),
      StructField("timeRange", schemaOf[TimeRange], true)
    ))

    protected def to[R](t: ItemDatetime, io: CatalystIO[R]): R = t match {
      case v: PointInTime => io.create(io.to(v), null)
      case v: TimeRange => io.create(null, io.to(v))
    }

    protected def from[R](t: R, io: CatalystIO[R]): ItemDatetime =
      Try(io.get[PointInTime](t, 0)).orElse(Try(io.get[TimeRange](t, 1))).get
  }

  implicit val stacProviderSerializer: CatalystSerializer[StacProvider] = new CatalystSerializer[StacProvider] {
    def schema: StructType = StructType(Seq(
      StructField("name", StringType, false),
      StructField("description", StringType, true),
      StructField("roles", ArrayType(StringType), false),
      StructField("url", StringType, true)
    ))

    protected def to[R](t: StacProvider, io: CatalystIO[R]): R = io.create(
      t.name.toUTF8String,
      t.description.map(_.toUTF8String).orNull,
      t.roles.map(_.repr.toUTF8String).toArrayData,
      t.url.map(_.toUTF8String).orNull
    )

    protected def from[R](t: R, io: CatalystIO[R]): StacProvider = StacProvider(
      name = io.getString(t, 0),
      description = io.getStringOption(t, 1),
      roles = io.getArray[UTF8String](t, 2).flatMap(_.toString.asJson.as[StacProviderRole].toOption).toList,
      url = io.getStringOption(t, 3)
    )
  }


  implicit val itemPropertiesSerializer: CatalystSerializer[ItemProperties] = new CatalystSerializer[ItemProperties] {
    def schema: StructType = StructType(Seq(
      StructField("datetime", schemaOf[ItemDatetime], false),
      StructField("title", StringType, true),
      StructField("description", StringType, true),
      StructField("created", TimestampType, true),
      StructField("updated", TimestampType, true),
      StructField("license", StringType, true),
      StructField("providers", ArrayType(schemaOf[StacProvider]), true),
      StructField("platform", StringType, true),
      StructField("instruments", ArrayType(StringType), true),
      StructField("constellation", StringType, true),
      StructField("mission", StringType, true),
      StructField("gsd", DoubleType, true),
      StructField("extensionFields", MapType(StringType, StringType), true)
    ))

    protected def to[R](t: ItemProperties, io: CatalystIO[R]): R = io.create(
      io.to(t.datetime),
      t.title.map(_.value.toUTF8String).orNull,
      t.description.map(_.value.toUTF8String).orNull,
      t.created.map(_.toEpochMilli).orNull,
      t.updated.map(_.toEpochMilli).orNull,
      t.license.map(_.asJson.spaces2.toUTF8String).orNull,
      t.providers.map(_.toList.map(io.to(_)).toArrayData).orNull,
      t.platform.map(_.value.toUTF8String).orNull,
      t.instruments.map(_.toList.map(_.value.toUTF8String).toArrayData).orNull,
      t.constellation.map(_.value.toUTF8String).orNull,
      t.mission.map(_.value.toUTF8String).orNull,
      t.gsd.orNull,
      t.extensionFields.toMap.toMapData
    )

    protected def from[R](t: R, io: CatalystIO[R]): ItemProperties = ItemProperties(
      datetime = io.get[ItemDatetime](t, 0),
      title = io.getNonEmptyString(t, 1),
      description = io.getNonEmptyString(t, 2),
      created = io.getInstantOption(t, 3),
      updated = io.getInstantOption(t, 4),
      license = io.getStringOption(t, 5).flatMap(_.asJson.as[StacLicense].toOption),
      providers = io.getArrayOption[StacProvider](t, 6).map(_.toList).flatMap(NonEmptyList.fromList),
      platform = io.getNonEmptyString(t, 7),
      instruments = io.getNonEmptyListNonEmptyString(t, 8),
      constellation = io.getNonEmptyString(t, 9),
      mission = io.getNonEmptyString(t, 10),
      gsd = io.getDoubleOption(t, 11),
      extensionFields = io.getMapOption[UTF8String, UTF8String](t, 12).map(_.toJsonObject).getOrElse(JsonObject.empty)
    )
  }


  implicit val stacItemSerializer: CatalystSerializer[StacItem] = new CatalystSerializer[StacItem] {
    def schema: StructType = StructType(Seq(
      StructField("id", StringType, false),
      StructField("stac_version", StringType, false),
      StructField("stac_extensions", ArrayType(StringType), false),
      StructField("geometry", schemaOf[Geometry], false),
      StructField("bbox", schemaOf[TwoDimBbox], false),
      StructField("links", ArrayType(schemaOf[StacLink])),
      StructField("assets", MapType(StringType, schemaOf[StacAsset])),
      StructField("collection", StringType, true),
      StructField("properties", schemaOf[ItemProperties], false)
    ))

    def to[R](t: StacItem, io: CatalystIO[R]): R = io.create(
      t.id.toUTF8String,
      t.stacVersion.toUTF8String,
      t.stacExtensions.map(_.toUTF8String).toArrayData,
      io.to(t.geometry),
      io.to(t.bbox),
      t.links.map(io.to(_)).toArrayData,
      ArrayBasedMapData(t.assets.map { case (k, v) => k.toUTF8String -> io.to(v) }),
      t.collection.map(_.toUTF8String).orNull,
      io.to(t.properties)
    )

    def from[R](t: R, io: CatalystIO[R]): StacItem = StacItem(
      id = io.getString(t, 0),
      stacVersion = io.getString(t, 1),
      stacExtensions = io.getArray[UTF8String](t, 2).map(_.toString).toList,
      _type = "Feature",
      geometry = io.get[Geometry](t, 3),
      bbox = io.get[TwoDimBbox](t, 4),
      links = io.getArray[StacLink](t, 5).toList,
      assets = io.getMap[UTF8String, StacAsset](t, 6).map { case (k, v) => k.toString -> v },
      collection = io.getStringOption(t, 7),
      properties = io.get[ItemProperties](t, 8)
    )
  }

  implicit val stacItemEncoder: ExpressionEncoder[StacItem] = CatalystSerializerEncoder[StacItem]()
}
