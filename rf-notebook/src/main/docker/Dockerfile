FROM s22s/pyspark-notebook:spark-2.3.3-hadoop-2.7

MAINTAINER Astraea, Inc.

ENV RF_LIB_LOC=/usr/local/rasterframes \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/conda/lib"

USER root

RUN mkdir $RF_LIB_LOC

EXPOSE 4040 4041 4042 4043 4044

RUN conda install --quiet --yes anaconda && conda update --all
RUN conda install --quiet --yes \
    sphinx nbsphinx shapely numpy folium geopandas geojsonio pyarrow arrow-cpp

# Cleanup pip residuals
RUN rm -rf /home/$NB_USER/.local && \
    fix-permissions /home/$NB_USER

# Note: The above step takes an insanely long time in the CONDA_DIR, so commenting it out until we have perm issues.
#    fix-permissions $CONDA_DIR

COPY *.whl $RF_LIB_LOC
RUN ls -1 $RF_LIB_LOC/*.whl | xargs pip install
COPY jupyter_notebook_config.py $HOME/.jupyter
COPY examples $HOME/examples
RUN chmod -R +w $HOME/examples

RUN chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID