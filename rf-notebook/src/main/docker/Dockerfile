FROM s22s/pyspark-notebook:spark-2.3.3-hadoop-2.7

MAINTAINER Astraea, Inc.

ENV RF_LIB_LOC /usr/local/rasterframes

USER root

RUN mkdir $RF_LIB_LOC

RUN echo "spark.driver.extraClassPath $RF_JAR" >> /usr/local/spark/conf/spark-defaults.conf  && \
    echo "spark.executor.extraClassPath $RF_JAR" >> /usr/local/spark/conf/spark-defaults.conf

EXPOSE 4040 4041 4042 4043 4044

ENV SPARK_OPTS $SPARK_OPTS \
    --driver-class-path $RF_JAR \
    --conf spark.executor.extraClassPath=$RF_JAR

ENV PYTHONPATH $PYTHONPATH:$PY_RF_ZIP

# Sphinx (for Notebook->html)
RUN conda install --quiet --yes \
    anaconda sphinx nbsphinx shapely numpy

# Cleanup pip residuals
RUN rm -rf /home/$NB_USER/.local && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# Do these after the standard environment setup
# since these change more regularly.
COPY *.whl $RF_LIB_LOC
RUN ls -1 $RF_LIB_LOC/*.whl | xargs pip install

RUN chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID

COPY jupyter_notebook_config.py $HOME/.jupyter

