.DEFAULT_GOAL := all
black = poetry run black peakina tests
isort = poetry run isort peakina tests

.PHONY: install_system_deps
install_system_deps:
	apt install -y libsnappy-dev

.PHONY: install
install:
	pip install -U pip
	pip install poetry
	poetry install
	poetry run pre-commit install

.PHONY: format
format:
	poetry run pre-commit run --all-files

.PHONY: lint
lint:
	poetry run flake8 peakina tests
	$(black) --diff --check
	$(isort) --check-only

.PHONY: mypy
mypy:
	poetry run mypy .

.PHONY: test
test:
	poetry run pytest --pull --cov=peakina --cov-report xml --cov-report term-missing

.PHONY: all
all: lint mypy test

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf .coverage build dist *.egg-info .pytest_cache .mypy_cache

.PHONY: build
build:
	poetry build

.PHONY: upload
upload:
	poetry publish
