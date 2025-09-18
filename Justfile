set dotenv-load := true

test-unit:
	poetry run pytest tests/unit

test-integration args:
	poetry run pytest tests/integration {{args}}

test args:
	poetry run pytest {{args}}