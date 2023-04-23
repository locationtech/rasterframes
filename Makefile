SHELL := env SPARK_VERSION=$(SPARK_VERSION) /usr/bin/env bash
SPARK_VERSION ?= 3.4.0

.PHONY: init test lint build docs notebooks help

help:
	@echo "init - Setup the repository"
	@echo "clean - clean all compiled python files, build artifacts and virtual envs. Run \`make init\` anew afterwards."
	@echo "test - run unit tests"
	@echo "lint - run linter and checks"
	@echo "build - build wheel"
	@echo "docs - build documentations"
	@echo "help - this command"

test: test-scala test-python

###############
# SCALA
###############

compile-scala:
	sbt -v -batch compile test:compile it:compile -DrfSparkVersion=${SPARK_VERSION}

test-scala: test-core-scala test-datasource-scala test-experimental-scala
	
test-core-scala:
	sbt -batch core/test -DrfSparkVersion=${SPARK_VERSION}

test-datasource-scala:
	sbt -batch datasource/test -DrfSparkVersion=${SPARK_VERSION}

test-experimental-scala:
	sbt -batch experimental/test -DrfSparkVersion=${SPARK_VERSION}

build-scala: clean-build-scala
	sbt "pyrasterframes/assembly" -DrfSparkVersion=${SPARK_VERSION}

clean-build-scala:
	find ./dist -name 'pyrasterframes-assembly-${SPARK_VERSION}*.jar' -exec rm -fr {} +

clean-scala:
	sbt clean

publish-scala:
	sbt publish -DrfSparkVersion=${SPARK_VERSION}

################
# PYTHON
################

init-python:
	python -m venv ./.venv
	./.venv/bin/python -m pip install --upgrade pip
	poetry self add "poetry-dynamic-versioning[plugin]"
	poetry install
	poetry add pyspark@${SPARK_VERSION}
	poetry run pre-commit install

test-python: build-scala
	poetry run pytest -vv python/tests --cov=python/pyrasterframes --cov=python/geomesa_pyspark --cov-report=term-missing

test-python-quick:
	poetry run pytest -vv python/tests --cov=python/pyrasterframes --cov=python/geomesa_pyspark --cov-report=term-missing

lint-python:
	poetry run pre-commit run --all-file

build-python: clean-build-python
	poetry build

docs-python: clean-docs-python
	poetry run python python/docs/build_docs.py

notebooks-python: clean-notebooks-python
	poetry run python python/docs/build_docs.py --format notebook

clean-python: clean-build-python clean-test-python clean-venv-python clean-docs-python clean-notebooks-python

clean-build-python:
	find ./dist -name 'pyrasterframes*.whl' -exec rm -fr {} +
	find ./dist -name 'pyrasterframes*.tar.gz' -exec rm -fr {} +

clean-test-python:
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr test*.pipe

clean-venv-python:
	rm -fr .venv/

clean-docs-python:
	find docs -name '*.md' -exec rm -f {} +

clean-notebooks-python:
	find docs -name '*.ipynb' -exec rm -f {} +
