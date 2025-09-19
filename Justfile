set dotenv-load := true

test-unit:
	poetry run pytest tests/unit

test-integration args:
	poetry run pytest tests/integration {{args}}

test args:
	poetry run pytest {{args}}

check:
	poetry run pylint data_transformations tests || poetry run mypy --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs data_transformations tests