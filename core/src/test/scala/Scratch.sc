import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTSFactoryFinder

val f = JTSFactoryFinder.getGeometryFactory

val pt = f.createPoint(new Coordinate(1, 2))

val poly = f.createPolygon(Array(new Coordinate(0, 0), new Coordinate(0, 3), new Coordinate(3, 3), new Coordinate(3, 0), new Coordinate(0, 0)))


poly.intersects(pt)
pt.intersects(poly)
pt.intersects(pt)


Double.NaN.toInt
