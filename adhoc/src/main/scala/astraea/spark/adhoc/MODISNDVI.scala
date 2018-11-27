/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package astraea.spark.adhoc

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util._
import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @since 11/24/18
 */
object MODISNDVI {

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .master(args.headOption.getOrElse("local[*]"))
      .appName(getClass.getName)
      .getOrCreate()
      .withRasterFrames

    import spark.sql

    sql("CREATE TEMPORARY VIEW modis USING `aws-pds-modis`" )
    sql("DESCRIBE modis").show(false)
//    sql("""
//       SELECT year(acquisition_date) as year, month(acquisition_date) as month, count(*) as granules
//       FROM modis
//       GROUP BY year, month
//       ORDER BY year, month
//        """).show()

//    sql(
//      """
//        SELECT DISTINCT explode(map_keys(assets)) as asset_keys
//        FROM modis
//        ORDER BY asset_keys
//      """).show()




    time("stats") {
      sql(
        """
        CREATE TEMPORARY VIEW red_nir_tiles_monthly_2017 AS
        SELECT granule_id, month(acquisition_date) as month, rf_read_tiles(assets['B01'], assets['B02']) as (crs, extent, red, nir)
        FROM (
           SELECT * from modis DISTRIBUTE BY granule_id
        )
        WHERE year(acquisition_date) = 2017 AND day(acquisition_date) = 15
          """)

      sql("DESCRIBE red_nir_tiles_monthly_2017").show(false)

      sql(
        """
        SELECT month, ndvi_stats.* FROM (
          SELECT month, rf_aggStats(rf_normalizedDifference(nir, red)) as ndvi_stats
          FROM red_nir_tiles_monthly_2017
          GROUP BY month
          ORDER BY month
        )
          """).show(false)
    }


    //      sql(
    //        """
    //        CREATE TEMPORARY VIEW red_nir_tiles_solstice AS
    //        SELECT year(acquisition_date) as year, rf_read_tiles(assets['B01'], assets['B02']) as (crs, extent, red, nir)
    //        FROM modis
    //        WHERE month(acquisition_date) = 6 AND day(acquisition_date) = 21 AND granule_id LIKE '%v05'
    //          """)
    //
    //      sql("DESCRIBE red_nir_tiles_solstice").show(false)
    //
    //      sql(
    //        """
    //        SELECT year, ndvi_stats.* FROM (
    //          SELECT year, rf_aggStats(rf_normalizedDifference(nir, red)) as ndvi_stats
    //          FROM red_nir_tiles_solstice
    //          GROUP BY year
    //          ORDER BY year
    //        )
    //          """).show(false)

    //sql("SELECT substring(granule_id, 0, 3) from modis").show()

    //    sql("""
    //      SELECT year(acquisition_date) as year, month(acquisition_date) as month, count(*) as granules
    //      FROM modis
    //      GROUP BY year, month
    //      ORDER BY year, month
    //      """).show()

    //    sql(
    //      """
    //         SELECT DISTINCT granule_id
    //         FROM modis
    //      """
    //    ).show()


      // NDVI
//    sql(
//      """
//        CREATE TEMPORARY VIEW ndvi AS
//        SELECT granule_id, rf_normalizedDifference(nir, red) as ndvi
//        FROM red_nir_tiles
//      """.stripMargin)

    // EVI2
//    sql(
//      """
//         CREATE TEMPORARY VIEW evi2 AS
//         SELECT granule_id, rf_localMultiplyScalar(
//           rf_localDivide(
//             rf_localSubtract(nir, red),
//             rf_localAddScalar(rf_localAdd(nir, rf_localMultiplyScalar(red, 2.4)), 1)
//           ), 2.5) as EVI2
//         FROM red_nir_tiles
//        """
//    )

//    sql(
//      """
//         SELECT rf_aggHistogram(ndvi)
//         FROM ndvi
//      """
//    ).show(false)

    //sql("SELECT count(*) from red_nir_tiles").show()
  }
}
