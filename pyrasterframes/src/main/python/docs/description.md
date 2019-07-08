# Description

We have a millennia-long history of organizing information in tabular form. Typically, rows represent independent events or observations, and columns represent measurements from the observations. The forms have evolved, from hand-written agricultural records and transaction ledgers, to the advent of spreadsheets on the personal computer, and on to the creation of the _DataFrame_ data structure as found in [R Data Frames][R] and [Python Pandas][Pandas]. The table-oriented data structure remains a common and critical component of organizing data across industries, and is the mental model employed by many data scientists across diverse forms of modeling and analysis. 

Today, DataFrames are the _lingua franca_ of data science. The evolution of the tabular form has continued with Apache Spark SQL, which brings DataFrames to the big data distributed compute space. Through several novel innovations, Spark SQL enables interactive and batch-oriented cluster computing without having to be versed in the highly specialized skills typically required for high-performance computing. As suggested by the name, these DataFrames are manipulatable via standard SQL, as well as the more general-purpose programming languages Python, R, Java, and Scala.

RasterFrames®, an incubating Eclipse Foundation LocationTech project, brings together Earth-observing (EO) data analysis, big data computing, and DataFrame-based data science. The recent explosion of EO data from public and private satellite operators presents both a huge opportunity as well as a challenge to the data analysis community. It is _Big Data_ in the truest sense, and its footprint is rapidly getting bigger. According to a World Bank document on assets for post-disaster situation awareness[^1]:

> Of the 1,738 operational satellites currently orbiting the earth (as of 9/[20]17), 596 are earth observation satellites and 477 of these are non-military assets (ie available to civil society including commercial entities and governments for earth observation, according to the Union of Concerned Scientists). This number is expected to increase significantly over the next ten years. The 200 or so planned remote sensing satellites have a value of over 27 billion USD (Forecast International). This estimate does not include the burgeoning fleets of smallsats as well as micro, nano and even smaller satellites... All this enthusiasm has, not unexpectedly, led to a veritable fire-hose of remotely sensed data which is becoming difficult to navigate even for seasoned experts.

RasterFrames provides a DataFrame-centric view over arbitrary EO data, enabling spatiotemporal queries, map algebra raster operations, and compatibility with the ecosystem of Spark ML algorithms. By using DataFrames as the core cognitive and compute data model, it is able to deliver these features in a form that is accessible to general analysts while handling the rapidly growing data footprint.

[R]:https://www.rdocumentation.org/packages/base/versions/3.5.1/topics/data.frame
[Pandas]:https://pandas.pydata.org/
[GeoJSON]:https://en.wikipedia.org/wiki/GeoJSON
[GTLayer]:https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html#layouts-and-tile-layers
[PDS]:https://registry.opendata.aws/modis/

[^1]: [_Demystifying Satellite Assets for Post-Disaster Situation Awareness_](https://docs.google.com/document/d/11bIw5HcEiZy8SKli6ZFQC2chVEiiIJ-f0o6btA4LU48).
World Bank via [OpenDRI.org](https://opendri.org/resource/demystifying-satellite-assets-for-post-disaster-situation-awareness/). Accessed November 28, 2018. 
