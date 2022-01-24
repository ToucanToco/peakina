.DEFAULT_GOAL := all
black = black peakina tests setup.py
isort = isort peakina tests setup.py

.PHONY: install_system_deps
install_system_deps:
	apt install -y libsnappy-dev

.PHONY: install
install:
	pip install -U setuptools pip
	pip install -e '.[test]'

.PHONY: format
format:
	${black}
	${isort}

.PHONY: lint
lint:
	flake8 peakina tests setup.py
	$(black) --diff --check
	$(isort) --check-only

.PHONY: mypy
mypy:
	mypy .

.PHONY: test
test:
	pytest --pull --cov=peakina --cov-report term-missing

.PHONY: all
all: lint mypy test

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
