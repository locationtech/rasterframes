FROM s22s/pyspark-notebook:spark-2.3.3-hadoop-2.7

MAINTAINER Astraea, Inc.

ENV RF_LIB_LOC=/usr/local/rasterframes \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/conda/lib"

USER root

RUN mkdir $RF_LIB_LOC

EXPOSE 4040 4041 4042 4043 4044

# Sphinx (for Notebook->html)
RUN conda install --quiet --yes \
    anaconda sphinx nbsphinx shapely numpy folium geopandas geojsonio rasterio descartes

# Cleanup pip residuals
RUN rm -rf /home/$NB_USER/.local && \
    fix-permissions /home/$NB_USER && \
    fix-permissions $CONDA_DIR

COPY *.whl $RF_LIB_LOC
COPY jupyter_notebook_config.py $HOME/.jupyter
COPY examples $HOME/examples

RUN ls -1 $RF_LIB_LOC/*.whl | xargs pip install
RUN chmod -R +w $HOME/examples && chown -R $NB_UID:$NB_GID $HOME

USER $NB_UID