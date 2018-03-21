val replesent = REPLesent(source="RasterFrames.txt", slideCounter=true, slideTotal=true, intp=$intp)
import replesent._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.ml.TileExploder
import geotrellis.raster.render._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import scala.sys.process._




