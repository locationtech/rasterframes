# jupyter/scipy-notebook isn't semantically versioned.
# We pick this arbitrary one from Sept 2019 because it's what latest was on Oct 17 2019.
FROM jupyter/scipy-notebook:1386e2046833

LABEL maintainer="Astraea, Inc. <info@astraea.earth>"

EXPOSE 4040 4041 4042 4043 4044

USER root

RUN \
    apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Spark dependencies
ENV APACHE_SPARK_VERSION 2.4.4
ENV HADOOP_VERSION 2.7
ENV APACHE_SPARK_CHECKSUM 2E3A5C853B9F28C7D4525C0ADCB0D971B73AD47D5CCE138C85335B9F53A6519540D3923CB0B5CEE41E386E49AE8A409A51AB7194BA11A254E037A848D0C4A9E5
ENV APACHE_SPARK_FILENAME spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ENV APACHE_SPARK_REMOTE_PATH spark-${APACHE_SPARK_VERSION}/${APACHE_SPARK_FILENAME}

RUN \
    cd /tmp && \
    wget --quiet http://apache.mirrors.pair.com/spark/${APACHE_SPARK_REMOTE_PATH} && \
    echo "${APACHE_SPARK_CHECKSUM} *${APACHE_SPARK_FILENAME}" | sha512sum -c - && \
    tar xzf ${APACHE_SPARK_FILENAME} -C /usr/local --owner root --group root --no-same-owner && \
    rm ${APACHE_SPARK_FILENAME}

RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Spark config
ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip
ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info

COPY conda_cleanup.sh .
RUN chmod u+x conda_cleanup.sh

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/conda/lib"
# Sphinx (for Notebook->html) and pyarrow (from pyspark build)
RUN \
    conda install --quiet --yes pyarrow \
    anaconda sphinx nbsphinx shapely numpy folium geopandas geojsonio rasterio descartes && \
    ./conda_cleanup.sh $NB_USER $CONDA_DIR

ENV RF_LIB_LOC=/usr/local/rasterframes
RUN mkdir $RF_LIB_LOC

COPY *.whl $RF_LIB_LOC
COPY jupyter_notebook_config.py $HOME/.jupyter
COPY examples $HOME/examples

RUN ls -1 $RF_LIB_LOC/*.whl | xargs pip install --no-cache-dir
RUN chmod -R +w $HOME/examples && chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID
