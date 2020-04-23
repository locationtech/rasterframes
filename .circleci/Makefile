IMAGE_NAME=miniconda-gdal
VERSION=latest
HOST=docker.pkg.github.com
REPO=${HOST}/locationtech/rasterframes
FULL_NAME=${REPO}/${IMAGE_NAME}:${VERSION}

all: build login push

build:
	docker build . -t ${FULL_NAME}

login:
	docker login ${HOST}

push:
	docker push ${FULL_NAME}

shell: build
	docker run --rm -it ${FULL_NAME} bash
