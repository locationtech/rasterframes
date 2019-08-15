// task to create documentation PDF
lazy val makePDF = taskKey[File]("Build PDF version of documentation")
lazy val pdfFileName = settingKey[String]("Name of the PDF file generated")
pdfFileName := s"RasterFrames-Users-Manual-${version.value}.pdf"

makePDF := {
  import scala.sys.process._

  // Get the python source directory configured in the root project.
  val base = (Compile / paradox / sourceDirectories).value.find(_.toString.contains("python")).head

  // Hard coded lacking any simple  way of determining order.
  val files = Seq(
    "index.md",
    "description.md",
    "concepts.md",
    "getting-started.md",
    "raster-io.md",
    "raster-catalogs.md",
    "raster-read.md",
    "raster-write.md",
    "vector-data.md",
    "raster-processing.md",
    "local-algebra.md",
    "nodata-handling.md",
    "aggregation.md",
    "time-series.md",
    "machine-learning.md",
    "unsupervised-learning.md",
    "supervised-learning.md",
    "numpy-pandas.md",
    "languages.md",
    "reference.md"
  ).map(base ** _).flatMap(_.get)

  val log = streams.value.log
  log.info("Section ordering:")
  files.foreach(f => log.info(" - " + f.getName))

  val work = target.value / "makePDF"
  work.mkdirs()

  val prepro = files.zipWithIndex.map { case (f, i) ⇒
    val dest = work / f"$i%02d.md"
    // Filter cross links and add a newline
    (Seq("sed", "-e", """s/@ref:\[\([^]]*\)\](.*)/_\1_/g;s/@@.*//g""", f.toString) #> dest).!
    // Add newline at the end of the file so as to make pandoc happy
    ("echo" #>> dest).!
    ("echo \\pagebreak" #>> dest).!
    dest
  }

  val output = target.value / pdfFileName.value

  val header = (Compile / sourceDirectory).value / "latex" / "header.latex"

  val args = "pandoc" ::
    "--from=markdown" ::
    "--to=pdf" ::
    "-t" :: "latex" ::
    "-s" ::
    "--toc" ::
    "-V" :: "title:RasterFrames Users' Manual" ::
    "-V" :: "author:Astraea, Inc." ::
    "-V" :: "geometry:margin=0.75in" ::
    "-V" :: "papersize:letter" ::
    "-V" :: "links-as-notes" ::
    "--include-in-header" :: header.toString ::
    "-o" :: output.toString ::
    prepro.map(_.toString).toList

  log.info(s"Running: ${args.mkString(" ")}")
  Process(args, base).!

  log.info("Wrote: " + output)

  output
}

makePDF := makePDF.dependsOn(Compile / paradox).value
