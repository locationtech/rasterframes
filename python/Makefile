SHELL := /usr/bin/env bash

.PHONY: init test lint build help

help:
	@echo "init - Setup the repository"
	@echo "clean - clean all compiled python files, build artifacts and virtual envs. Run \`make init\` anew afterwards."
	@echo "test - run unit tests"
	@echo "lint - run linter and checks"
	@echo "build - build wheel"
	@echo "help - this command"

init:
	python -m venv .venv
	./.venv/bin/python -m pip install --upgrade pip
	poetry install
	poetry run pre-commit install

test: clean init
	poetry run pytest -vv tests --cov=h3xtiles --cov-report=term-missing

lint:
	poetry run pre-commit run --all-file

build: clean-build
	poetry build

clean: clean-build clean-pyc clean-test clean-venv

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr wheelhouse/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.so' -exec rm -f {} +
	find . -name '*.c' -exec rm -f {} +
	find . -name '*.html' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -f .coverage
	rm -fr htmlcov/

clean-venv:
		rm -fr .venv/
