/*
 * Copyright (c) 2019 Astraea, Inc. All right reserved.
 */

package org.locationtech.rasterframes.datasource.tiles

import better.files.File
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{functions => F}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.scalatest.BeforeAndAfter

class TilesDataSourceSpec extends TestEnvironment with TestData with BeforeAndAfter {
  val baseDir = File("target") / "tiles"

  def mkOutdir(prefix: String) = {
    val resultsDir = baseDir.createDirectories()
    File.newTemporaryDirectory(prefix, Some(resultsDir))
  }

  describe("Tile writing") {

    def tileFiles(dir: File, ext: String = ".tif") =
      dir.listRecursively.filter(f => f.extension.contains(ext))

    def countTiles(dir: File, ext: String = ".tif"): Int = tileFiles(dir, ext).length

    lazy val df = spark.read.raster
      .from(Seq(cogPath, l8B1SamplePath, nonCogPath))
      .withLazyTiles(false)
      .withTileDimensions(128, 128)
      .load()
      .cache()

    it("should write tiles with defaults") {
      df.count() should be > 0L
      val dest = mkOutdir("defaults-")
      df.write.tiles.save(dest.toString)
      countTiles(dest) should be(df.count())
    }

    it("should write png tiles") {
      df.count() should be > 0L
      val dest = mkOutdir("png-")
      df.write.tiles.asPNG.withCatalog.save(dest.toString)
      countTiles(dest, ".png") should be(df.count())
    }

    it("should write tiles with custom filename") {
      val dest = mkOutdir("filename-")
      val df2 = df
        .withColumn("filename", F.concat_ws("-", F.lit("bunny"), F.monotonically_increasing_id()))

      df2.write.tiles
        .withFilenameColumn("filename")
        .save(dest.toString)

      countTiles(dest) should be(df.count)

      forAll(tileFiles(dest).toSeq) { p =>
        p.toString should include("bunny")
        p.toString should endWith(".tif")
      }
    }

    it("should support arbitrary subdirectories in filename and generate a catalog with metadata") {
      val dest = mkOutdir("subdirs-")
      val df2 = df
        .withColumn("label", F.when(F.rand() > 0.5, "cat").otherwise("dog"))
        .withColumn("testval", F.when(F.rand() > 0.5, "test").otherwise("train"))
        .withColumn(
          "filename",
          F.concat_ws("/", col("label"), col("testval"), F.monotonically_increasing_id())
        )
        .repartition(col("filename"))

      df2.write.tiles
        .withFilenameColumn("filename")
        .withMetadataColumns("label", "testval")
        .withCatalog
        .save(dest.toString)

      countTiles(dest) should be(df.count())

      val cat = dest / "catalog.csv"
      cat.exists should be(true)

      cat.lineIterator.exists(_.contains("testval")) should be(true)
      cat.lineIterator.exists(_.contains("dog")) should be(true)
      cat.lineIterator.exists(_.contains("+proj=utm")) should be(true)

      val sample = tileFiles(dest).next()
      val tags = SinglebandGeoTiff(sample.toString()).tags.headTags
      tags.keys should contain("testval")
    }
  }

  override def additionalConf(conf: SparkConf) =
    conf.set("spark.debug.maxToStringFields", "100")
}
