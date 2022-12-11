/*
 * Copyright (c) 2020 Astraea, Inc. All right reserved.
 */

package org.locationtech.rasterframes.datasource.slippy

import better.files._
import org.apache.spark.sql.functions.col
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.scalatest.BeforeAndAfterAll

class SlippyDataSourceSpec extends TestEnvironment with TestData with BeforeAndAfterAll {
  val baseDir = File("target") / "slippy"

  override def beforeAll() = {
    super.beforeAll()
    baseDir.delete(swallowIOExceptions = true)
  }

  def countFiles(dir: File, extension: String): Int = {
    dir.list(f => f.isRegularFile && f.name.endsWith(extension)).length
  }

  // When running in the IDE on MacOS, launch viewer pages for visual evaluation.
  def view(dir: File): Unit = {
    def isIntelliJ = sys.props.get("sun.java.command").exists(_.contains("jetbrains"))
    if (isIntelliJ && System.getProperty("os.name").contains("Mac")) {
      import scala.sys.process._
      val openCommand = s"open ${(dir / "index.html").canonicalPath}"
      openCommand.!
    }
  }

  def tileFilesCount(dir: File): Long = {
    val r = countFiles(dir, ".png")
    println(dir, r)
    r
  }

  def mkOutdir(prefix: String) = {
    val resultsDir = baseDir.createDirectories()
    File.newTemporaryDirectory(prefix, Some(resultsDir))
  }

  val l8RGBPath = Resource.getUrl("LC08_RGB_Norfolk_COG.tiff").toURI

  describe("Slippy writing") {
    lazy val rf = spark.read.raster
      .from(Seq(l8RGBPath))
      .withLazyTiles(false)
      .withTileDimensions(128, 128)
      .withBandIndexes(0, 1, 2)
      .load()
      .withColumnRenamed("proj_raster_b0", "red")
      .withColumnRenamed("proj_raster_b1", "green")
      .withColumnRenamed("proj_raster_b2", "blue")
      .cache()

    it("should write a singleband") {
      val dir = mkOutdir("single-")
      rf.select(col("red"))
        .write.slippy.withHTML.save(dir.toString)
      tileFilesCount(dir) should be (155L)
      view(dir)
    }

    it("should write with non-uniform coloring") {
      val dir = mkOutdir("quick-")
      rf.select(col("green"))
        .write.slippy.withColorRamp("BlueToOrange")
        .withHTML.save(dir.toString)

      tileFilesCount(dir) should be (155L)
      view(dir)
    }

    it("should write with uniform coloring") {
      val dir = mkOutdir("uniform-")
      rf.select(col("green"))
        .write.slippy
        .withColorRamp("Viridis")
        .withUniformColor
        .withHTML.save(dir.toString)

      tileFilesCount(dir) should be (155L)
      view(dir)
    }
    it("should write greyscale") {
      val dir = mkOutdir("relation-hist-noramp-")
      rf.select(col("green"))
        .write.slippy
        .withUniformColor
        .withHTML
        .save(dir.toString)

      tileFilesCount(dir) should be (155L)
      view(dir)
    }

    it("Should write colour composite") {
      val dir = mkOutdir("color-")
      rf.write.slippy
        .withUniformColor
        .withHTML
        .save(dir.toString())
      tileFilesCount(dir) should be (155L)
      view(dir)
    }

    it("should construct map on a file in the wild") {
      val modisUrl = "https://modis-pds.s3.us-west-2.amazonaws.com/MCD43A4.006/27/05/2020161/MCD43A4.A2020161.h27v05.006.2020170060718_B01.TIF"
      val modisRf = spark.read.raster.from(Seq(modisUrl))
        .withLazyTiles(false)
        .load()
      val dir = mkOutdir("modis-")
      modisRf.write.slippy
        .withUniformColor
        .withHTML
        .save(dir.toString())
      tileFilesCount(dir) should be (210L)
      view(dir)
    }

    ignore("should write non-homogenous cell types") {
      val dir = mkOutdir(s"mixed-celltypes-")
      noException should be thrownBy {
        rf.select(rf_log(col("red")), col("green"), col("blue"))
          .write.slippy.withHTML.save(dir.toString)
      }

      tileFilesCount(dir) should be (151L)
      view(dir)
    }
  }
}

// Runner to support profiling.
//object SlippyDataSourceSpec {
//  def main(args: Array[String]): Unit = {
//    import org.scalatest._
//    run(new SlippyDataSourceSpec, testName = "Should write colour composite")
//  }
//}
