moduleName := "raster-frames-datasource"

libraryDependencies ++= Seq(
  geotrellis("s3").value,
  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided
)

// Run generateDocs to help convert examples to tut docs.
//docsMap := Map(baseDirectory.value / "src" / "test" -> target.value / "literator" )


initialCommands in console := """
                                |import astraea.spark.rasterframes._
                                |import geotrellis.raster._
                                |import geotrellis.spark.io.kryo.KryoRegistrator
                                |import org.apache.spark.serializer.KryoSerializer
                                |import org.apache.spark.sql._
                                |import org.apache.spark.sql.functions._
                                |import astraea.spark.rasterframes.datasource.geotrellis._
                                |import astraea.spark.rasterframes.datasource.geotiff._
                                |implicit val spark = SparkSession.builder()
                                |    .master("local[*]")
                                |    .appName(getClass.getName)
                                |    .config("spark.serializer", classOf[KryoSerializer].getName)
                                |    .config("spark.kryoserializer.buffer.max", "500m")
                                |    .config("spark.kryo.registrationRequired", "false")
                                |    .config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
                                |    .getOrCreate()
                                |    .withRasterFrames
                                |spark.sparkContext.setLogLevel("ERROR")
                                |import spark.implicits._
                                |
""".stripMargin

cleanupCommands in console := """
                                |spark.stop()
                              """.stripMargin