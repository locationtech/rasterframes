package org.apache.spark.sql.stac

import org.locationtech.jts.geom._
import org.apache.spark.sql.jts.AbstractGeometryUDT
import org.locationtech.jts.geom.Geometry

class PointUDT extends AbstractGeometryUDT[Point]("point")
class MultiPointUDT extends AbstractGeometryUDT[MultiPoint]("multipoint")
class LineStringUDT extends AbstractGeometryUDT[LineString]("linestring")
class MultiLineStringUDT extends AbstractGeometryUDT[MultiLineString]("multilinestring")
class PolygonUDT extends AbstractGeometryUDT[Polygon]("polygon")
class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon]("multipolygon")
class GeometryUDT extends AbstractGeometryUDT[Geometry]("geometry")
class GeometryCollectionUDT extends AbstractGeometryUDT[GeometryCollection]("geometrycollection")