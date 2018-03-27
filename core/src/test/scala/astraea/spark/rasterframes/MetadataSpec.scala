package astraea.spark.rasterframes

import org.apache.spark.sql.types.MetadataBuilder

/**
 * Test rig for column metadata management.
 *
 * @since 9/6/17
 */
class MetadataSpec extends TestEnvironment with TestData  {
  import spark.implicits._

  private val sampleMetadata = new MetadataBuilder().putBoolean("haz", true).putLong("baz", 42).build()

  describe("Metadata storage") {
    it("should serialize and attach metadata") {
      //val rf = sampleGeoTiff.projectedRaster.toRF(128, 128)
      val df = spark.createDataset(Seq((1, "one"), (2, "two"), (3, "three"))).toDF("num", "str")
      val withmeta = df.mapColumnAttribute($"num", attr ⇒ {
        attr.withMetadata(sampleMetadata)
      })

      val meta2 = withmeta.fetchMetadataValue($"num", _.metadata)
      assert(Some(sampleMetadata) === meta2)
    }

    it("should handle post-join duplicate column names") {
      val df1 = spark.createDataset(Seq((1, "one"), (2, "two"), (3, "three"))).toDF("num", "str")
      val df2 = spark.createDataset(Seq((1, "a"), (2, "b"), (3, "c"))).toDF("num", "str")
      val joined = df1.as("a").join(df2.as("b"), "num")

      val withmeta = joined.mapColumnAttribute(df1("str"), attr ⇒ {
        attr.withMetadata(sampleMetadata)
      })

      val meta2 = withmeta.fetchMetadataValue($"str", _.metadata)

      assert(Some(sampleMetadata) === meta2)
    }
  }
}
