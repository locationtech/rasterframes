FROM jupyter/scipy-notebook:latest

MAINTAINER Astraea, Inc.

EXPOSE 4040 4041 4042 4043 4044

ENV RF_LIB_LOC=/usr/local/rasterframes \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/conda/lib"

USER root

RUN mkdir $RF_LIB_LOC

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Spark dependencies
ENV APACHE_SPARK_VERSION 2.3.4
ENV HADOOP_VERSION 2.7

RUN cd /tmp && \
        wget -q http://apache.mirrors.pair.com/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
        echo "9FBEFCE2739990FFEDE6968A9C2F3FE399430556163BFDABDF5737A8F9E52CD535489F5CA7D641039A87700F50BFD91A706CA47979EE51A3A18787A92E2D6D53 *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
        tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
        rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Spark config
ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip
ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info

# Sphinx (for Notebook->html) and pyarrow (from pyspark build)
RUN conda install --quiet --yes pyarrow \
    anaconda sphinx nbsphinx shapely numpy folium geopandas geojsonio rasterio descartes \
    && conda clean --all \
    && rm -rf /home/$NB_USER/.local \
    && find /opt/conda/ -type f,l -name '*.a' -delete \
    && find /opt/conda/ -type f,l -name '*.pyc' -delete \
    && find /opt/conda/ -type f,l -name '*.js.map' -delete \
    && find /opt/conda/lib/python*/site-packages/bokeh/server/static -type f,l -name '*.js' -not -name '*.min.js' -delete \
    && rm -rf /opt/conda/pkgs \
    && fix-permissions $CONDA_DIR \
    && fix-permissions /home/$NB_USER

COPY *.whl $RF_LIB_LOC
COPY jupyter_notebook_config.py $HOME/.jupyter
COPY examples $HOME/examples

RUN ls -1 $RF_LIB_LOC/*.whl | xargs pip install --no-cache-dir
RUN chmod -R +w $HOME/examples && chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID
