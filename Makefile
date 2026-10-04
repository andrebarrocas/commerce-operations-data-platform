.PHONY: install test lint types contracts demo spark-test check clean

install:
	python -m pip install -e '.[dev,aws]'

test:
	python -m pytest -m 'not spark' -q

lint:
	python -m ruff check src tests scripts spark
	python -m ruff format --check src tests scripts spark

types:
	python -m mypy src/commerce_platform

contracts:
	python scripts/check_contracts.py

demo:
	commerce demo --orders 1000 --directory var/demo

spark-test:
	RUN_SPARK_TESTS=1 SPARK_LOCAL_IP=127.0.0.1 python -m pytest -m spark -q

check: contracts test lint types

clean:
	@echo 'Generated data is under var/. Remove a selected run only after verifying its retained input and manifests.'
