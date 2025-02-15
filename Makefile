PHONY: protobuf-test, lint, format import-sort


protobuf-test:
	python ./memc_load.py --test

lint:
	ruff check

format:
	black .

import-sort:
	isort .
