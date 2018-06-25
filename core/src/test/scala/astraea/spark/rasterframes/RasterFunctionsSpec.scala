package astraea.spark.rasterframes

import astraea.spark.rasterframes.TestData.randomTile
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.sql.functions._

class RasterFunctionsSpec extends TestEnvironment with TestData {

  import TestData.injectND
  import sqlContext.implicits._

  describe("computing tile-wise summaries") {
    it("should report sum") {
      val ds = Seq.fill[Tile](10)(byteArrayTile).toDF("tile")
      val expected = byteArrayTile.toArray().sum

      ds.select(tileSum($"tile")).collect()
        .foreach(result ⇒ assert(result === expected))

    }

  }

}
