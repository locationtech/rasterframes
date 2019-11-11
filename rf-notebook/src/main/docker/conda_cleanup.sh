#!/bin/bash

NB_USER=$1
CONDA_DIR=$2
conda clean --all --force-pkgs-dirs --yes && \
    rm -rf /home/$NB_USER/.local && \
    find /opt/conda/ -type f,l -name '*.a' -delete && \
    find /opt/conda/ -type f,l -name '*.pyc' -delete && \
    find /opt/conda/ -type f,l -name '*.js.map' -delete && \
    find /opt/conda/lib/python*/site-packages/bokeh/server/static -type f,l -name '*.js' -not -name '*.min.js' -delete && \
    rm -rf /opt/conda/pkgs && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER