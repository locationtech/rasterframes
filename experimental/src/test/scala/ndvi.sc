import geotrellis.proj4.LatLng
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


implicit val spark = SparkSession.builder().
  master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

val modis = spark.read.format("aws-pds-modis-catalog").load()

val red_nir_monthly_2017 = modis
  .select($"granule_id", month($"acquisition_date") as "month", $"B01" as "red", $"B02" as "nir")
  .where(year($"acquisition_date") === 2017 && (dayofmonth($"acquisition_date") === 15) && $"granule_id" === "h21v09")

val red_nir_tiles_monthly_2017 = spark.read.raster
  .fromCatalog(red_nir_monthly_2017, "red", "nir")
  .load()

val result = red_nir_tiles_monthly_2017
  .where(st_intersects(
    st_reproject(rf_geometry($"red"), rf_crs($"red"), LatLng),
    st_makePoint(34.870605, -4.729727)
  ))
  .groupBy("month")
  .agg(rf_agg_stats(rf_normalized_difference($"nir", $"red")) as "ndvi_stats")
  .orderBy("month")
  .select("month", "ndvi_stats.*")


result.show()