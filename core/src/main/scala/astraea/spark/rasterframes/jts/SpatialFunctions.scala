package astraea.spark.rasterframes.jts

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.jts._
import org.apache.spark.sql.{Column, Encoder, TypedColumn}

/**
 * Spatial type support functions.
 *
 * @since 2/19/18
 */
trait SpatialFunctions {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._

  /** Constructs a geometric literal from a value  and JTS UDT */
  private def udtlit[T >: Null <: Geometry: Encoder, U <: AbstractGeometryUDT[T]](t: T, u: U): TypedColumn[Any, T] =
    new Column(Literal.create(u.serialize(t), u)).as[T]

  /** Create a generic geometry literal, encoded as a GeometryUDT. */
  def geomLit(g: Geometry): TypedColumn[Any, Geometry] = udtlit(g, GeometryUDT)
  /** Create a point literal, encoded as a PointUDT. */
  def pointLit(g: Point): TypedColumn[Any, Point] = udtlit(g, PointUDT)
  /** Create a line literal, encoded as a LineUDT. */
  def lineLit(g: LineString): TypedColumn[Any, LineString] = udtlit(g, LineStringUDT)
  /** Create a polygon literal, encoded as a PolygonUDT. */
  def polygonLit(g: Polygon): TypedColumn[Any, Polygon] = udtlit(g, PolygonUDT)
  /** Create a multi-point literal, encoded as a MultiPointUDT. */
  def mPointLit(g: MultiPoint): TypedColumn[Any, MultiPoint] = udtlit(g, MultiPointUDT)
  /** Create a multi-line literal, encoded as a MultiPointUDT. */
  def mLineLit(g: MultiLineString): TypedColumn[Any, MultiLineString] = udtlit(g, MultiLineStringUDT)
  /** Create a multi-polygon literal, encoded as a MultiPolygonUDT. */
  def mPolygonLit(g: MultiPolygon): TypedColumn[Any, MultiPolygon] = udtlit(g, MultiPolygonUDT)
  /** create a geometry collection literal, encoded as a GeometryCollectionUDT. */
  def geomCollLit(g: GeometryCollection): TypedColumn[Any, GeometryCollection] = udtlit(g, GeometryCollectionUDT)
}
