package org.locationtech.rasterframes.datasource.stac.api.encoders

import com.azavea.stac4s.ItemDatetime
import frameless.SQLTimestamp
import cats.syntax.option._

import java.time.Instant

case class ItemDatetimeCatalyst(start: SQLTimestamp, end: Option[SQLTimestamp], _type: ItemDatetimeCatalystType)

object ItemDatetimeCatalyst {
  def toDatetime(dt: ItemDatetimeCatalyst): ItemDatetime = {
    val ItemDatetimeCatalyst(start, endo, _type) = dt
    (_type, endo) match {
      case (ItemDatetimeCatalystType.PointInTime, _) => ItemDatetime.PointInTime(Instant.ofEpochMilli(start.us))
      case (ItemDatetimeCatalystType.TimeRange, Some(end)) => ItemDatetime.TimeRange(Instant.ofEpochMilli(start.us), Instant.ofEpochMilli(end.us))
      case err => throw new Exception(s"ItemDatetimeCatalyst decoding is not possible, $err")
    }
  }

  def fromItemDatetime(dt: ItemDatetime): ItemDatetimeCatalyst = dt match {
    case ItemDatetime.PointInTime(when) =>
      ItemDatetimeCatalyst(SQLTimestamp(when.toEpochMilli), None, ItemDatetimeCatalystType.PointInTime)
    case ItemDatetime.TimeRange(start, end) =>
      ItemDatetimeCatalyst(SQLTimestamp(start.toEpochMilli), SQLTimestamp(end.toEpochMilli).some, ItemDatetimeCatalystType.PointInTime)
  }
}
