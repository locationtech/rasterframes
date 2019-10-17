FROM circleci/openjdk:8-jdk

ENV OPENJPEG_VERSION 2.3.1
ENV GDAL_VERSION 2.4.1
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

# most of these libraries required for
# python-pip pandoc && pip install setuptools => required for pyrasterframes testing
RUN \
    sudo apt-get update && \
    sudo apt remove \
      python python-minimal python2.7 python2.7-minimal \
      libpython-stdlib libpython2.7 libpython2.7-minimal libpython2.7-stdlib \
      && \
    sudo apt-get install -y \
      pandoc wget \
      gcc g++ build-essential bash-completion cmake imagemagick \
      libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev \
      liblzma-dev libcurl4-gnutls-dev libproj-dev libgeos-dev libhdf4-alt-dev libpng-dev libffi-dev \
      && \
    sudo apt autoremove && \
    sudo apt-get clean all

RUN \
    cd /tmp && \
    wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz && \
    tar xzf Python-3.7.4.tgz && \
    cd Python-3.7.4 && \
    ./configure  --with-ensurepip=install --prefix=/usr/local --enable-optimization && \
    make && \
    sudo make altinstall && \
    rm -rf Python-3.7.4*

RUN \
    sudo ln -s /usr/local/bin/python3.7 /usr/local/bin/python && \
    sudo curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    sudo python get-pip.py && \
    sudo pip3 install setuptools ipython==6.2.1

# install OpenJPEG
RUN \
    cd /tmp && \
    wget https://github.com/uclouvain/openjpeg/archive/v${OPENJPEG_VERSION}.tar.gz && \
    tar -xf v${OPENJPEG_VERSION}.tar.gz && \
    cd openjpeg-${OPENJPEG_VERSION}/ && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local/ && \
    make -j && \
    sudo make install && \
    cd /tmp && rm -Rf v${OPENJPEG_VERSION}.tar.gz openjpeg*

# Compile and install GDAL with Java bindings
RUN \
    cd /tmp && \
    wget http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz && \
    tar -xf gdal-${GDAL_VERSION}.tar.gz && \
    cd gdal-${GDAL_VERSION} && \
    ./configure \
        --with-curl \
        --with-hdf4 \
        --with-geos \
        --with-geotiff=internal \
        --with-hide-internal-symbols \
        --with-libtiff=internal \
        --with-libz=internal \
        --with-mrf \
        --with-openjpeg \
        --with-threads \
        --without-jp2mrsid \
        --without-netcdf \
        --without-ecw && \
    make -j 8 && \
    sudo make install && \
    sudo ldconfig && \
    cd /tmp && sudo rm -Rf gdal*
