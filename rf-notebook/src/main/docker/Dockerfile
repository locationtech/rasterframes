# jupyter/scipy-notebook isn't semantically versioned.
# We pick this arbitrary one from Sept 2019 because it's what latest was on Oct 17 2019.
FROM jupyter/scipy-notebook:7a0c7325e470

LABEL maintainer="Astraea, Inc. <info@astraea.earth>"

USER root

RUN \
    apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Spark dependencies
ENV APACHE_SPARK_VERSION 2.4.5
ENV HADOOP_VERSION 2.7
ENV APACHE_SPARK_CHECKSUM 2426a20c548bdfc07df288cd1d18d1da6b3189d0b78dee76fa034c52a4e02895f0ad460720c526f163ba63a17efae4764c46a1cd8f9b04c60f9937a554db85d2
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