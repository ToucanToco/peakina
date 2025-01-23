.DEFAULT_GOAL := all
# Commands
RUFF   = uv run ruff check peakina tests
FORMAT = uv run ruff format peakina tests
MYPY   = uv run mypy peakina tests

.PHONY: install_system_deps
install_system_deps:
	apt install -y libsnappy-dev

.PHONY: install
install:
	uv sync --all-extras
	uv run pre-commit install

.PHONY: format
format:
	${RUFF} --fix
	${FORMAT}

.PHONY: lint
lint:
	${RUFF}
	${FORMAT} --check
	${MYPY}

.PHONY: mypy
mypy:
	uv run mypy .

.PHONY: test
test:
	uv run pytest --pull --cov=peakina --cov-report xml --cov-report term-missing

.PHONY: all
all: lint mypy test

.PHONY: doc
doc:
	uv run mkdocs serve

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf .coverage build dist *.egg-info .pytest_cache .mypy_cache
