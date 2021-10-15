package org.locationtech.rasterframes.datasource.stac.api.encoders

import cats.data.Ior
import frameless.SQLTimestamp
import cats.syntax.option._
import com.azavea.stac4s.{PointInTime, TimeRange}
import com.azavea.stac4s.types.ItemDatetime

import java.time.Instant

case class ItemDatetimeCatalyst(datetime: Option[SQLTimestamp], start: Option[SQLTimestamp], end: Option[SQLTimestamp], _type: ItemDatetimeCatalystType)

object ItemDatetimeCatalyst {
  def toDatetime(dt: ItemDatetimeCatalyst): ItemDatetime = {
    dt match {
      case ItemDatetimeCatalyst(Some(datetime), Some(start), Some(end), ItemDatetimeCatalystType.PointInTimeAndTimeRange) =>
        Ior.Both(PointInTime(Instant.ofEpochMilli(datetime.us)), TimeRange(Instant.ofEpochMilli(start.us), Instant.ofEpochMilli(end.us)))
      case ItemDatetimeCatalyst(Some(datetime), _, _, ItemDatetimeCatalystType.PointInTime) =>
        Ior.Left(PointInTime(Instant.ofEpochMilli(datetime.us)))
      case ItemDatetimeCatalyst(_, Some(start), Some(end), ItemDatetimeCatalystType.PointInTime) =>
        Ior.Right(TimeRange(Instant.ofEpochMilli(start.us), Instant.ofEpochMilli(end.us)))
      case e => throw new Exception(s"ItemDatetimeCatalyst decoding is not possible, $e")
    }
  }

  def fromItemDatetime(dt: ItemDatetime): ItemDatetimeCatalyst = dt match {
    case Ior.Left(PointInTime(datetime)) =>
      ItemDatetimeCatalyst(SQLTimestamp(datetime.toEpochMilli).some, None, None, ItemDatetimeCatalystType.PointInTime)
    case Ior.Right(TimeRange(start, end)) =>
      ItemDatetimeCatalyst(None, SQLTimestamp(start.toEpochMilli).some, SQLTimestamp(end.toEpochMilli).some, ItemDatetimeCatalystType.PointInTime)
    case Ior.Both(PointInTime(datetime), TimeRange(start, end)) =>
      ItemDatetimeCatalyst(SQLTimestamp(datetime.toEpochMilli).some, SQLTimestamp(start.toEpochMilli).some, SQLTimestamp(end.toEpochMilli).some, ItemDatetimeCatalystType.PointInTime)
  }
}
