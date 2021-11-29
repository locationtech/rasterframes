/*
 * Copyright (c) 2020 Astraea, Inc. All right reserved.
 */

package org.locationtech.rasterframes.datasource.slippy

import geotrellis.layer.{SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.slippy.HadoopSlippyTileWriter
import geotrellis.vector.reproject.Implicits._
import org.apache.commons.text.StringSubstitutor
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes.expressions.aggregates.ProjectedLayerMetadataAggregate
import org.locationtech.rasterframes.util.withResource
import org.locationtech.rasterframes.{rf_agg_approx_histogram, _}
import org.locationtech.rasterframes.datasource._

import java.io.PrintStream
import java.net.URI
import java.nio.file.Paths
import scala.io.Source
import RenderingProfiles._
import org.locationtech.rasterframes.datasource.slippy.RenderingModes.{RenderingMode, Uniform}

object DataFrameSlippyExport extends StandardEncoders {
  val destCRS = WebMercator

  /**
   * Export tiles as a slippy map.
   * NB: Temporal components are ignored blindly.
   *
   * @param dest    URI for Hadoop supported storage endpoint (e.g. 'file://', 'hdfs://', etc.).
   * @param profile Rendering profile
   */
  def writeSlippyTiles(df: DataFrame, dest: URI, profile: Profile): SlippyResult = {

    val spark = df.sparkSession
    implicit val sc = spark.sparkContext

    val outputPath: String = dest.toASCIIString

    require(
      df.tileColumns.length >= profile.expectedBands, // TODO: Do we want to allow this greater than case? Warn the user?
      s"Selected rendering mode '${profile}' expected ${profile.expectedBands} bands.")

    // select only the tile columns given by user and crs, extent columns which are fallback if first `column` is not a PRT
    val SpatialComponents(crs, extent, dims, cellType) = projectSpatialComponents(df)
      .getOrElse(
        throw new IllegalArgumentException("Provided dataframe did not have an Extent and/or CRS"))

    val tlm: TileLayerMetadata[SpatialKey] =
      df.select(
          ProjectedLayerMetadataAggregate(
            destCRS,
            extent,
            crs,
            cellType,
            dims
          )
        )
        .first()

    val rfLayer = df
      .toLayer(tlm)
      // TODO: this should be fixed in RasterFrames
      .na
      .drop()
      .persist()
      .asInstanceOf[RasterFrameLayer]

    val inputRDD: MultibandTileLayerRDD[SpatialKey] =
      rfLayer.toMultibandTileLayerRDD match {
        case Left(spatial) => spatial
        case Right(_) =>
          throw new NotImplementedError(
            "Dataframes with multiple temporal values are not yet supported.")
      }

    val tileColumns = rfLayer.tileColumns

    val rp = profile match {
      case up: UniformColorRampProfile =>
        val hist = rfLayer
          .select(rf_agg_approx_histogram(tileColumns.head))
          .first()
        up.toResolvedProfile(hist)
      case up: UniformRGBColorProfile =>
        require(tileColumns.length >= 3)
        val stats = rfLayer
          .select(
            rf_agg_stats(tileColumns(0)),
            rf_agg_stats(tileColumns(1)),
            rf_agg_stats(tileColumns(2)))
          .first()
        up.toResolvedProfile(stats._1, stats._2, stats._3)
      case o => o
    }

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val (zoom, reprojected) = inputRDD.reproject(WebMercator, layoutScheme, Bilinear)
    val renderer = (_: SpatialKey, tile: MultibandTile) => rp.render(tile).bytes
    val writer = new HadoopSlippyTileWriter[MultibandTile](outputPath, "png")(renderer)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      writer.write(z, rdd)
    }

    rfLayer.unpersist()

    val center = reprojected.metadata.extent.center
      .reproject(WebMercator, LatLng)

    SlippyResult(dest, center.getY, center.getX, zoom)
  }

  def writeSlippyTiles(df: DataFrame, dest: URI, renderingMode: RenderingMode): SlippyResult = {

    val profile = (df.tileColumns.length, renderingMode) match {
      case (1, Uniform) => UniformColorRampProfile(greyscale)
      case (_, Uniform) => UniformRGBColorProfile()
      case (1, _) => ColorRampProfile(greyscale)
      case _ => RGBColorProfile()
    }
    writeSlippyTiles(df, dest, profile)
  }

  def writeSlippyTiles(df: DataFrame, dest: URI, colorRamp: ColorRamp, renderingMode: RenderingMode): SlippyResult = {
    val profile = renderingMode match {
      case Uniform ⇒ UniformColorRampProfile(colorRamp)
      case _ ⇒ ColorRampProfile(colorRamp)
    }
    writeSlippyTiles(df, dest, profile)
  }

  case class SlippyResult(dest: URI, centerLat: Double, centerLon: Double, maxZoom: Int) {
    // for python interop
    def outputUrl(): String = dest.toASCIIString

    def writeHtml(spark: SparkSession): Unit = {
      import java.util.{HashMap => JMap}

      val subst = new StringSubstitutor(new JMap[String, String]() {
        put("maxNativeZoom", maxZoom.toString)
        put("id", Paths.get(dest.getPath).getFileName.toString)
        put("viewLat", centerLat.toString)
        put("viewLon", centerLon.toString)
      })

      val rawLines = Source.fromInputStream(getClass.getResourceAsStream("/slippy.html")).getLines()

      val fs = FileSystem.get(dest, spark.sparkContext.hadoopConfiguration)

      withResource(fs.create(new Path(new Path(dest), "index.html"), true)) { hout =>
        val out = new PrintStream(hout, true, "UTF-8")
        for (line <- rawLines) {
          out.println(subst.replace(line))
        }
      }
    }
  }
}
