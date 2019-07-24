# Machine Learning

RasterFrames provides facilities to train and predict with a wide variety of machine learning models through [Spark ML Pipelines](https://spark.apache.org/docs/latest/ml-guide.html). This library provides a variety of pipeline components for supervised learning, unsupervised learning, and data preparation that can be used to represent and repeatably conduct a variety of tasks in machine learning.

Furthermore, through the use of [`pandas_udf`s](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#pandas-udfs-aka-vectorized-udfs), `Tile`s in RasterFrames can be piped into deep learning frameworks, such as [Keras](https://keras.io/) or [TensorFlow](https://www.tensorflow.org/).

The following sections provide some examples on how to integrate these workflows with RasterFrames.

@@@ index

* @ref:[Unsupervised Machine Learning](unsupervised-learning.md)
* @ref:[Supervised Machine Learning](supervised-learning.md)
* @ref:[Deep Learning](deep-learning.md)

@@@
