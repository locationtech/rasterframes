FROM s22s/pyspark-notebook:spark-2.3.3-hadoop-2.7

MAINTAINER Astraea, Inc.

ENV RF_LIB_LOC /usr/local/rasterframes

USER root

RUN mkdir $RF_LIB_LOC

EXPOSE 4040 4041 4042 4043 4044

# Sphinx (for Notebook->html)
RUN conda install --quiet --yes \
    anaconda sphinx nbsphinx shapely numpy folium geopandas geojsonio

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

COPY examples $HOME/examples
