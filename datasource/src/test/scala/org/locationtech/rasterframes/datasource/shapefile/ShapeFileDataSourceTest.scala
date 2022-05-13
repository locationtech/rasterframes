package org.locationtech.rasterframes.datasource.shapefile

import geotrellis.shapefile.ShapeFileReader
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.TestEnvironment

import java.net.URL

class ShapeFileDataSourceTest extends TestEnvironment { self =>
  import spark.implicits._

  describe("ShapeFile Spark reader") {
    it("should read a shapefile") {
      val url = "https://github.com/locationtech/geotrellis/raw/master/shapefile/data/shapefiles/demographics/demographics.shp"
      import ShapeFileReader._

      val expected = ShapeFileReader
        .readSimpleFeatures(new URL(url))
        .map(_.geom[Geometry])
        .take(2)

      val results =
        spark
          .read
          .format("shapefile")
          .option("url", url)
          .load()
          .limit(2)

      results.printSchema()

      results.as[Option[Geometry]].collect() shouldBe expected
    }
  }
}
