IMAGE_NAME=circleci-openjdk-conda-gdal
SHA=$(shell git log -n1 --format=format:"%H" | cut -c 1-7)
VERSION?=$(SHA)
HOST=docker.pkg.github.com
REPO=$(HOST)/locationtech/rasterframes
FULL_NAME=$(REPO)/$(IMAGE_NAME):$(VERSION)
GIT_USER?=metasim
KEY?=$(HOME)/.github/repo-publish-key.txt

.DEFAULT_GOAL := help
help:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@echo "Usage: make [target]"
	@echo "Targets: "
	@grep -E '^[a-zA-Z0-9_%/-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\t\033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: build push ## Build and then push image

build: ## Build the docker image
	docker build . -t ${FULL_NAME}

login: ## Login to the docker registry
	cat $(KEY) | docker login $(HOST) -u $(GIT_USER) --password-stdin

push: login ## Push docker image to registry
	docker push ${FULL_NAME}

run: build ## Build image and launch shell
	docker run --rm -it ${FULL_NAME} bash
