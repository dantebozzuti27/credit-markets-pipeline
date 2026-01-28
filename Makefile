.PHONY: help install install-dev test lint format typecheck clean docker-up docker-down migrate run

PYTHON := python3
PYTEST := pytest
RUFF := ruff

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	$(PYTHON) -m pip install -e .

install-dev: ## Install development dependencies
	$(PYTHON) -m pip install -e ".[dev]"
	pre-commit install

test: ## Run all tests
	$(PYTEST) tests/ -v --cov=src/credit_markets --cov-report=term-missing

test-unit: ## Run unit tests only
	$(PYTEST) tests/unit/ -v -m unit

test-integration: ## Run integration tests (requires Docker)
	$(PYTEST) tests/integration/ -v -m integration

test-e2e: ## Run end-to-end tests
	$(PYTEST) tests/e2e/ -v -m e2e

lint: ## Run linter
	$(RUFF) check src/ tests/

lint-fix: ## Fix linting issues
	$(RUFF) check src/ tests/ --fix

format: ## Format code
	$(RUFF) format src/ tests/

typecheck: ## Run type checker
	mypy src/

check: lint typecheck test-unit ## Run all checks (lint, typecheck, unit tests)

clean: ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".coverage" -delete

docker-up: ## Start local infrastructure
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	sleep 5
	@echo "Services ready!"

docker-down: ## Stop local infrastructure
	docker-compose down -v

docker-logs: ## Show docker logs
	docker-compose logs -f

migrate: ## Run database migrations
	alembic upgrade head

migrate-new: ## Create a new migration
	@read -p "Migration name: " name; \
	alembic revision --autogenerate -m "$$name"

run: ## Run the pipeline for today
	$(PYTHON) -m credit_markets.cli run --date $(shell date +%Y-%m-%d)

run-backfill: ## Backfill historical data
	@read -p "Start date (YYYY-MM-DD): " start; \
	read -p "End date (YYYY-MM-DD): " end; \
	$(PYTHON) -m credit_markets.cli backfill --start-date $$start --end-date $$end

validate: ## Run data quality checks
	$(PYTHON) -m credit_markets.cli validate

api: ## Start the API server
	uvicorn credit_markets.api.main:app --reload --host 0.0.0.0 --port 8000

dashboard: ## Start the Streamlit dashboard
	streamlit run dashboard/app.py

# Docker build targets
docker-build: ## Build production Docker image
	docker build -t credit-markets-pipeline:latest -f infrastructure/docker/Dockerfile .

docker-push: ## Push to container registry
	docker tag credit-markets-pipeline:latest $(ECR_REPO):latest
	docker push $(ECR_REPO):latest
