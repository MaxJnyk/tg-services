# Makefile
.PHONY: up down restart logs test lint format clean dev dlq-stats dlq-list

up:
	docker-compose up -d

down:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f worker

test:
	pytest tests/ -v

test-integration:
	pytest tests/integration/ -v

lint:
	ruff check src/
	mypy src/

format:
	ruff format src/

clean:
	docker-compose down -v
	rm -rf .pytest_cache .mypy_cache .ruff_cache

# Dev: запуск worker локально (postgres и redis через docker)
dev: up
	taskiq worker src.entrypoints.broker:broker --workers 2 --fs-discover

# DLQ management
dlq-stats:
	python -m src.cli.dlq stats

dlq-list:
	python -m src.cli.dlq list
