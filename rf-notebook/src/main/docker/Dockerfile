# Python version compatible with Spark 3.1.x and GDAL 3.1.2
FROM jupyter/scipy-notebook:python-3.8.8

LABEL maintainer="Astraea, Inc. <info@astraea.earth>"

USER root

RUN \
    apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV APACHE_SPARK_VERSION 3.1.2
ENV HADOOP_VERSION 3.2
# On MacOS compute this with `shasum -a 512`
ARG APACHE_SPARK_CHECKSUM="2385cb772f21b014ce2abd6b8f5e815721580d6e8bc42a26d70bbcdda8d303d886a6f12b36d40f6971b5547b70fae62b5a96146f0421cb93d4e51491308ef5d5"
ARG APACHE_SPARK_FILENAME="spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
ARG APACHE_SPARK_REMOTE_PATH="spark-${APACHE_SPARK_VERSION}/${APACHE_SPARK_FILENAME}"

RUN \
    cd /tmp && \
    wget --quiet https://archive.apache.org/dist/spark/${APACHE_SPARK_REMOTE_PATH} && \
    echo "${APACHE_SPARK_CHECKSUM} *${APACHE_SPARK_FILENAME}" | sha512sum -c -

RUN \
    cd /tmp && \
    tar xzf ${APACHE_SPARK_FILENAME} -C /usr/local --owner root --group root --no-same-owner && \
    rm ${APACHE_SPARK_FILENAME}

RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Spark config
ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info
ENV RF_LIB_LOC=/usr/local/rasterframes

COPY conda_cleanup.sh requirements-nb.txt $RF_LIB_LOC/
RUN chmod u+x $RF_LIB_LOC/conda_cleanup.sh

RUN \
    conda config --set unsatisfiable_hints True && \
    conda --debug update --channel conda-forge --all --yes --quiet && \
    conda install --yes --channel conda-forge --file $RF_LIB_LOC/requirements-nb.txt && \
    $RF_LIB_LOC/conda_cleanup.sh $NB_USER $CONDA_DIR

RUN conda list --export

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/conda/lib"
COPY *.whl $RF_LIB_LOC/
COPY jupyter_notebook_config.py $HOME/.jupyter/
COPY examples $HOME/examples

RUN ls -1 $RF_LIB_LOC/*.whl | xargs pip install --no-cache-dir
RUN chmod -R +w $HOME/examples && chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID

EXPOSE 4040 4041 4042 4043 4044