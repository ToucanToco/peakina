.DEFAULT_GOAL := all

.PHONY: install
install:
	pip install -U setuptools pip
	pip install -e '.[test]'

.PHONY: format
format:
	isort -rc peakina tests setup.py
	black peakina tests setup.py

.PHONY: lint
lint:
	flake8 peakina tests setup.py
	black --check peakina tests setup.py

.PHONY: mypy
mypy:
	mypy peakina

.PHONY: test
test:
	pytest --cov=peakina --cov-report term-missing

.PHONY: all
all: test mypy lint

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf .coverage build dist *.egg-info .pytest_cache .mypy_cache

.PHONY: build
build:
	python setup.py sdist bdist_wheel

.PHONY: upload
upload:
	twine upload dist/*
