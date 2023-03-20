SHELL := /usr/bin/env bash

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
	sbt -v -batch compile test:compile it:compile

test-scala: test-core-scala test-datasource-scala test-experimental-scala
	
test-core-scala:
	sbt -batch core/test

test-datasource-scala:
	sbt -batch datasource/test

test-experimental-scala:
	sbt -batch experimental/test

build-scala:
	sbt "pyrasterframes/assembly"
	mkdir -p python/pyrasterframes/jars/
	cp pyrasterframes/target/scala-2.12/pyrasterframes-assembly-*.jar python/pyrasterframes/jars/

clean-scala:
	sbt clean

publish-scala:
	sbt publish

################
# PYTHON
################

init-python:
	python -m venv ./.venv
	./.venv/bin/python -m pip install --upgrade pip
	poetry install
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

clean-python: clean-build-python clean-pyc-python clean-test-python clean-venv-python clean-docs-python clean-notebooks-python

clean-build-python:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr wheelhouse/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.so' -exec rm -f {} +
	find . -name '*.c' -exec rm -f {} +
	find . -name '*.html' -exec rm -f {} +

clean-pyc-python:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

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
