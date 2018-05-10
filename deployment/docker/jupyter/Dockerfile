FROM jupyter/pyspark-notebook:92fe05d1e7e5

MAINTAINER Astraea

ENV RF_LIB_LOC /usr/lib
ENV PY_RF_JAR $RF_LIB_LOC/pyrasterframes.jar
ENV PY_RF_ZIP $RF_LIB_LOC/pyrasterframes.zip
ENV PY_RF_DATA $HOME/samples/

COPY *.zip $PY_RF_ZIP
COPY *.jar $PY_RF_JAR
COPY *.ipynb $HOME/

USER root

RUN echo "spark.driver.extraClassPath $PY_RF_JAR" >> /usr/local/spark/conf/spark-defaults.conf  && \
    echo "spark.executor.extraClassPath $PY_RF_JAR" >> /usr/local/spark/conf/spark-defaults.conf

RUN mkdir $PY_RF_DATA
COPY ./samples/* $PY_RF_DATA
RUN chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID

ENV SPARK_OPTS $SPARK_OPTS \
    --py-files $PY_RF_ZIP \
    --jars $PY_RF_JAR \
    --driver-class-path $PY_RF_JAR \
    --conf spark.executor.extraClassPath=$PY_RF_JAR

ENV PYTHONPATH $PYTHONPATH:$PY_RF_ZIP


#================================
# Copied from all-spark-notebook
#================================

# TODO:     resolve the issue that toree has with --py-files, above (it does not like .zips and
# TODO:     the kernel will not start)
# Apache Toree kernel
#RUN pip install --no-cache-dir \
#    https://dist.apache.org/repos/dist/dev/incubator/toree/0.2.0-incubating-rc5/toree-pip/toree-0.2.0.tar.gz \
#    && \
#    jupyter toree install --sys-prefix && \
#    rm -rf /home/$NB_USER/.local && \
#    fix-permissions $CONDA_DIR && \
#    fix-permissions /home/$NB_USER

# Spylon-kernel
RUN conda install --quiet --yes 'spylon-kernel=0.4*' && \
    conda clean -tipsy && \
    python -m spylon_kernel install --sys-prefix && \
    rm -rf /home/$NB_USER/.local && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER
