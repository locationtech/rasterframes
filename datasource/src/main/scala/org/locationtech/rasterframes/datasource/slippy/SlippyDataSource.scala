/*
 * Copyright (c) 2020 Astraea, Inc. All right reserved.
 */

package org.locationtech.rasterframes.datasource.slippy

import geotrellis.raster.render.ColorRamp
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.locationtech.rasterframes.util.ColorRampNames
import DataFrameSlippyExport._
import org.locationtech.rasterframes.datasource
import org.locationtech.rasterframes.datasource.slippy.RenderingModes.{Fast, RenderingMode, Uniform}

import java.net.URI

class SlippyDataSource extends DataSourceRegister with CreatableRelationProvider {
  import SlippyDataSource._
  override def shortName(): String = SHORT_NAME
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val pathURI = parameters.path.getOrElse(throw new IllegalArgumentException("Valid URI 'path' parameter required."))

    // TODO: How to make use of these? Looked through Spark sourcer and it's not clear
    // how one properly implements these so they work in a distributed context.
    mode match {
      case SaveMode.Append => ()
      case SaveMode.Overwrite => ()
      case SaveMode.ErrorIfExists =>()
      case SaveMode.Ignore => ()
    }
    val info = parameters.colorRamp match {
      case Some(cr) =>
        writeSlippyTiles(data, pathURI, cr, parameters.renderingMode)
      case _ =>
        writeSlippyTiles(data, pathURI, parameters.renderingMode)
    }

    if (parameters.withHTML)
      info.writeHtml(sqlContext.sparkSession)

    // The current function is called by `org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand`, which
    // ignores the return value. It in turn returns `Seq.empty[Row]`... ¯\_(ツ)_/¯
    null
  }
}


object SlippyDataSource {
  final val SHORT_NAME = "slippy"
  final val PATH_PARAM = "path"
  final val COLOR_RAMP_PARAM = "colorramp"
  final val HTML_PARAM = "html"
  final val RENDERING_MODE_PARAM = "renderingmode"

  implicit class SlippyDictAccessors(val parameters: Map[String, String]) extends AnyVal {
    def path: Option[URI] = datasource.uriParam(PATH_PARAM, parameters)
    def colorRamp: Option[ColorRamp] = parameters.get(COLOR_RAMP_PARAM).flatMap {
      case ColorRampNames(ramp) => Some(ramp)
      case _ => None
    }
    def renderingMode: RenderingMode = parameters.get(RENDERING_MODE_PARAM).map(_.toLowerCase()) match {
      case Some("uniform") | Some("histogram") ⇒ Uniform
      case _ ⇒ Fast
    }
    def withHTML: Boolean = parameters.get(HTML_PARAM).exists(_.toBoolean)
  }
}
