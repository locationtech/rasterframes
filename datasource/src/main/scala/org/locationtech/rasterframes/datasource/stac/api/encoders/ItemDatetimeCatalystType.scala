package org.locationtech.rasterframes.datasource.stac.api.encoders

sealed trait ItemDatetimeCatalystType { lazy val repr: String = this.getClass.getName.split("\\$").last }
object ItemDatetimeCatalystType {
  case object PointInTime extends ItemDatetimeCatalystType
  case object TimeRange extends ItemDatetimeCatalystType
  case object PointInTimeAndTimeRange extends ItemDatetimeCatalystType

  def fromString(str: String): ItemDatetimeCatalystType = str match {
    case PointInTime.repr => PointInTime
    case TimeRange.repr   => TimeRange
    case str              => throw new IllegalArgumentException(s"ItemDatetimeCatalystType can't be created from $str")
  }
}
