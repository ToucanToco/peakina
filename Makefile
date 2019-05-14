.DEFAULT_GOAL := all

.PHONY: install
install:
	pip install -U setuptools pip
	pip install -U -r requirements.txt
	pip install -e .

.PHONY: format
format:
	isort -rc -w 100 peakina tests
	black -S -l 100 --py36 peakina tests

.PHONY: lint
lint:
	flake8 peakina/ tests/
#	pytest tests -p no:sugar -q
	black -S -l 100 --target-version py36 --check peakina tests

.PHONY: mypy
mypy:
	mypy --follow-imports=skip --ignore-missing-imports --no-strict-optional peakina

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
