FROM circleci/openjdk:8-jdk

ENV MINICONDA_VERSION=4.8.2 \
    MINICONDA_MD5=87e77f097f6ebb5127c77662dfc3165e \
    CONDA_VERSION=4.8.2 \
    CONDA_DIR=/opt/conda \
    PYTHON_VERSION=3.7.7

USER root

ENV PATH=$CONDA_DIR/bin:$PATH

# circleci is 3434
COPY --chown=3434:3434 fix-permissions /tmp

RUN \
    apt-get update && \
    apt-get install -yq --no-install-recommends \
        sudo \
        wget \
        bzip2 \
        file \
        libtinfo5 \
        ca-certificates \
        gettext-base \
        locales && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN \
    cd /tmp && \
    mkdir -p $CONDA_DIR && \
    wget --quiet https://repo.continuum.io/miniconda/Miniconda3-py37_${MINICONDA_VERSION}-Linux-x86_64.sh && \
    echo "${MINICONDA_MD5} *Miniconda3-py37_${MINICONDA_VERSION}-Linux-x86_64.sh" | md5sum -c - && \
    /bin/bash Miniconda3-py37_${MINICONDA_VERSION}-Linux-x86_64.sh -f -b -p $CONDA_DIR && \
    rm Miniconda3-py37_${MINICONDA_VERSION}-Linux-x86_64.sh && \
    conda config --system --set auto_update_conda false && \
    conda config --system --set show_channel_urls true && \
    conda config --system --set channel_priority strict && \
    if [ ! $PYTHON_VERSION = 'default' ]; then conda install --yes python=$PYTHON_VERSION; fi && \
    conda list python | grep '^python ' | tr -s ' ' | cut -d '.' -f 1,2 | sed 's/$/.*/' >> $CONDA_DIR/conda-meta/pinned && \
    conda install --quiet --yes conda && \
    conda install --quiet --yes pip && \
    pip config set global.progress_bar off && \
    echo "$CONDA_DIR/lib" > /etc/ld.so.conf.d/conda.conf && \
    conda clean --all --force-pkgs-dirs --yes --quiet && \
    sh /tmp/fix-permissions $CONDA_DIR 2> /dev/null

COPY requirements-conda.txt /tmp/

RUN \
    conda install --channel conda-forge --no-channel-priority --freeze-installed \
        --file /tmp/requirements-conda.txt && \
    conda clean --all --force-pkgs-dirs --yes --quiet && \
    sh /tmp/fix-permissions $CONDA_DIR 2> /dev/null && \
    ldconfig 2> /dev/null

# Work-around for pyproj issue https://github.com/pyproj4/pyproj/issues/415
ENV PROJ_LIB=/opt/conda/share/proj

USER 3434

WORKDIR /home/circleci
