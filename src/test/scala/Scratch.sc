import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.util.Filesystem
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollection
import spray.json.DefaultJsonProtocol._
import spray.json.JsonReader

def parseFeatures[G <: Geometry : JsonReader,D <: Product: JsonReader](jsonString: String) : Seq[Feature[G,D]] = {
  val jfc  = jsonString.parseGeoJson[JsonFeatureCollection]
  jfc.getAllFeatures[Feature[G,D]]
}

val tiff = SinglebandGeoTiff(getClass.getResource("/L8-B2-Elkton-VA.tiff").getPath)
val json = Filesystem.readText(getClass.getResource("/L8-Labels-Elkton-VA.geojson").getPath)//

json.extractFeatures[Feature[Geometry, Map[String, String]]]()

//implicit val foo = jsonFormat1[Tuple1[Int]]
parseFeatures[Polygon, Tuple1[Int]](json)
