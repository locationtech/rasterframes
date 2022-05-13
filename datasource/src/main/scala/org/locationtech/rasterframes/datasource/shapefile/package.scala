package org.locationtech.rasterframes.datasource

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.jts.geom.Geometry

package object shapefile extends Serializable {
  // see org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
  // GeometryUDT should be registered before the encoder below is used
  // TODO: use TypedEncoders derived from UDT instances?
  @transient implicit lazy val geometryExpressionEncoder: ExpressionEncoder[Option[Geometry]] = ExpressionEncoder()
}
