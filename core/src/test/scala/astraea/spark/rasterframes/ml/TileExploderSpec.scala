package astraea.spark.rasterframes.ml

import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.raster.Tile
import org.apache.spark.sql.functions.lit
/**
 *
 * @since 2/16/18
 */
class TileExploderSpec extends TestEnvironment with TestData {
  describe("Tile explode transformer") {
    import spark.implicits._
    it("should explode tiles") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2").withColumn("other", lit("stuff"))

      val exploder = new TileExploder()
      val newSchema = exploder.transformSchema(df.schema)

      val exploded = exploder.transform(df)
      assert(newSchema === exploded.schema)
      assert(exploded.columns.length === 5)
      assert(exploded.count() === 9)
      write(exploded)
    }
  }
}
