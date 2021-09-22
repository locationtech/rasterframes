FROM circleci/openjdk:11-jdk
#LABEL org.opencontainers.image.source=https://github.com/locationtech/rasterframes

USER root

# See: https://docs.conda.io/projects/conda/en/latest/user-guide/install/rpm-debian.html
RUN \
  curl -s https://repo.anaconda.com/pkgs/misc/gpgkeys/anaconda.asc | gpg --dearmor > conda.gpg  && \
  install -o root -g root -m 644 conda.gpg /usr/share/keyrings/conda-archive-keyring.gpg && \
  gpg --keyring /usr/share/keyrings/conda-archive-keyring.gpg --no-default-keyring --fingerprint 34161F5BF5EB1D4BFBBB8F0A8AEB4F8B29D82806 && \
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/conda-archive-keyring.gpg] https://repo.anaconda.com/pkgs/misc/debrepo/conda stable main" > /etc/apt/sources.list.d/conda.list

RUN \
  apt-get update && \
  apt-get install -yq --no-install-recommends conda && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ENV CONDA_DIR=/opt/conda
ENV PATH=$CONDA_DIR/bin:$PATH

COPY requirements-conda.txt fix-permissions /tmp
RUN \
    conda install --quiet --yes --channel=conda-forge --file=/tmp/requirements-conda.txt && \
    echo "$CONDA_DIR/lib" > /etc/ld.so.conf.d/conda.conf && \
    ldconfig && \
    conda clean --all --force-pkgs-dirs --yes --quiet && \
    sh /tmp/fix-permissions $CONDA_DIR


# Work-around for pyproj issue https://github.com/pyproj4/pyproj/issues/415
ENV PROJ_LIB=/opt/conda/share/proj

USER 3434
WORKDIR /home/circleci
