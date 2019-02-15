FROM jupyter/pyspark-notebook:92fe05d1e7e5

MAINTAINER Astraea, Inc.

ENV RF_LIB_LOC /usr/lib
ENV RF_JAR $RF_LIB_LOC/rasterframes.jar
ENV PY_RF_ZIP $RF_LIB_LOC/pyrasterframes.zip

USER root

RUN echo "spark.driver.extraClassPath $RF_JAR" >> /usr/local/spark/conf/spark-defaults.conf  && \
    echo "spark.executor.extraClassPath $RF_JAR" >> /usr/local/spark/conf/spark-defaults.conf

EXPOSE 4040 4041 4042 4043 4044

ENV SPARK_OPTS $SPARK_OPTS \
    --py-files $PY_RF_ZIP \
    --jars $RF_JAR \
    --driver-class-path $RF_JAR \
    --conf spark.executor.extraClassPath=$RF_JAR

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
    python -m spylon_kernel install --sys-prefix 

# Sphinx (for Notebook->html)
RUN conda install --quiet --yes \
    sphinx nbsphinx 

# Cleanup pip residuals
RUN rm -rf /home/$NB_USER/.local && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# Do these after the standard environment setup
# since these change more regularly.
COPY *.zip $PY_RF_ZIP
COPY *.jar $RF_JAR

RUN chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID

# RUN pip install guzzle_sphinx_theme

# TODO: This repo can change regularly without docker knowing that the
# TODO: Layer this command is written in has become stale. Need to either
# TODO: clone a specific revision that we manually update, or keep this
# TODO: last, assuming the prior commends will be detected as stale.
RUN git clone http://github.com/s22s/rasterframes-book && ln -s rasterframes-book/Python/samples
