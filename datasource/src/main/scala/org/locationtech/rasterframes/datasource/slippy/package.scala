/*
 * Copyright (c) 2020 Astraea, Inc. All right reserved.
 */

package org.locationtech.rasterframes.datasource

import org.apache.spark.sql.DataFrameWriter
import org.locationtech.rasterframes.util.ColorRampNames
import shapeless.tag.@@

package object slippy {
    trait SlippyDataFrameWriterTag

    type SlippyDataFrameWriter[T] = DataFrameWriter[T] @@ SlippyDataFrameWriterTag

    /** Adds `slippy` format specifier to `DataFrameWriter`. */
    implicit class DataFrameWriterHasSlippyFormat[T](val reader: DataFrameWriter[T]) {
      def slippy: SlippyDataFrameWriter[T] =
        shapeless.tag[SlippyDataFrameWriterTag][DataFrameWriter[T]](
          reader.format(SlippyDataSource.SHORT_NAME))
    }

    /** Adds option methods relevant to SlippyDataSource. */
    implicit class SlippyDataFrameWriterHasOptions[T](val writer: SlippyDataFrameWriter[T]) {
      private def checkCM(colorRampName: String): Unit =
        require(ColorRampNames.unapply(colorRampName).isDefined,
          s"'$colorRampName' does was not found in ${ColorRampNames().mkString(",")}'")

      def withColorRamp(colorRampName: String): SlippyDataFrameWriter[T] = {
        checkCM(colorRampName)
        shapeless.tag[SlippyDataFrameWriterTag][DataFrameWriter[T]](
          writer.option(SlippyDataSource.COLOR_RAMP_PARAM, colorRampName)
        )
      }

      def withUniformColor: SlippyDataFrameWriter[T] = {
        shapeless.tag[SlippyDataFrameWriterTag][DataFrameWriter[T]](
          writer.option(SlippyDataSource.RENDERING_MODE_PARAM, RenderingModes.Uniform.toString)
        )
      }

      def withHTML: SlippyDataFrameWriter[T] = {
        shapeless.tag[SlippyDataFrameWriterTag][DataFrameWriter[T]](
          writer.option(SlippyDataSource.HTML_PARAM, true.toString)
        )
      }
    }
}
