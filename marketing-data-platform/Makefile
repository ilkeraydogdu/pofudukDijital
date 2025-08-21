# Makefile for Marketing Data Platform

.PHONY: help build up down restart logs clean test crawl transform serve dev prod

# Variables
DOCKER_COMPOSE = docker-compose
DOCKER_COMPOSE_DEV = docker-compose -f docker-compose.yml -f docker-compose.dev.yml
PYTHON = python3
PIP = pip3

# Colors for output
RED = \033[0;31m
GREEN = \033[0;32m
YELLOW = \033[1;33m
NC = \033[0m # No Color

help: ## Show this help message
	@echo "$(GREEN)Marketing Data Platform - Available Commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

# Environment setup
install: ## Install dependencies locally
	@echo "$(GREEN)Installing Python dependencies...$(NC)"
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)Installing Playwright browsers...$(NC)"
	playwright install chromium
	@echo "$(GREEN)Installation complete!$(NC)"

env: ## Copy environment variables template
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(GREEN).env file created. Please update with your API keys.$(NC)"; \
	else \
		echo "$(YELLOW).env file already exists.$(NC)"; \
	fi

# Docker commands
build: ## Build Docker images
	@echo "$(GREEN)Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) build

up: ## Start all services
	@echo "$(GREEN)Starting services...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo "API: http://localhost:8000"
	@echo "Airflow: http://localhost:8080"
	@echo "OpenSearch Dashboards: http://localhost:5601"
	@echo "Grafana: http://localhost:3000"
	@echo "MinIO: http://localhost:9001"

down: ## Stop all services
	@echo "$(YELLOW)Stopping services...$(NC)"
	$(DOCKER_COMPOSE) down

restart: ## Restart all services
	@echo "$(YELLOW)Restarting services...$(NC)"
	$(DOCKER_COMPOSE) restart

logs: ## Show logs from all services
	$(DOCKER_COMPOSE) logs -f

logs-api: ## Show API logs
	$(DOCKER_COMPOSE) logs -f api

logs-airflow: ## Show Airflow logs
	$(DOCKER_COMPOSE) logs -f airflow-scheduler airflow-webserver

# Development commands
dev: ## Start development environment
	@echo "$(GREEN)Starting development environment...$(NC)"
	$(DOCKER_COMPOSE_DEV) up -d
	@echo "$(GREEN)Development environment started!$(NC)"
	@echo "Jupyter: http://localhost:8888"
	@echo "pgAdmin: http://localhost:5050"
	@echo "MailHog: http://localhost:8025"

dev-down: ## Stop development environment
	@echo "$(YELLOW)Stopping development environment...$(NC)"
	$(DOCKER_COMPOSE_DEV) down

shell-api: ## Open shell in API container
	$(DOCKER_COMPOSE) exec api /bin/bash

shell-db: ## Open PostgreSQL shell
	$(DOCKER_COMPOSE) exec postgres psql -U marketing_user -d marketing_platform

# Data pipeline commands
crawl: ## Run data collection crawler
	@echo "$(GREEN)Starting data collection...$(NC)"
	$(DOCKER_COMPOSE) exec api python -m src.collectors.run_collection

transform: ## Run dbt transformations
	@echo "$(GREEN)Running dbt transformations...$(NC)"
	$(DOCKER_COMPOSE) run --rm dbt dbt run --project-dir /app/dbt

test-transform: ## Test dbt models
	@echo "$(GREEN)Testing dbt models...$(NC)"
	$(DOCKER_COMPOSE) run --rm dbt dbt test --project-dir /app/dbt

quality-check: ## Run data quality checks
	@echo "$(GREEN)Running data quality checks...$(NC)"
	$(DOCKER_COMPOSE) exec api python -m src.quality.run_checks

# Database commands
db-init: ## Initialize database schema
	@echo "$(GREEN)Initializing database...$(NC)"
	$(DOCKER_COMPOSE) exec postgres psql -U marketing_user -d marketing_platform -f /docker-entrypoint-initdb.d/init.sql

db-migrate: ## Run database migrations
	@echo "$(GREEN)Running database migrations...$(NC)"
	$(DOCKER_COMPOSE) exec api alembic upgrade head

db-backup: ## Backup database
	@echo "$(GREEN)Backing up database...$(NC)"
	@mkdir -p backups
	$(DOCKER_COMPOSE) exec postgres pg_dump -U marketing_user marketing_platform | gzip > backups/backup_$$(date +%Y%m%d_%H%M%S).sql.gz
	@echo "$(GREEN)Backup completed!$(NC)"

db-restore: ## Restore database from latest backup
	@echo "$(YELLOW)Restoring database from latest backup...$(NC)"
	@gunzip -c $$(ls -t backups/*.sql.gz | head -1) | $(DOCKER_COMPOSE) exec -T postgres psql -U marketing_user -d marketing_platform
	@echo "$(GREEN)Database restored!$(NC)"

# Search index commands
index-create: ## Create OpenSearch indices
	@echo "$(GREEN)Creating OpenSearch indices...$(NC)"
	curl -X PUT "localhost:9200/companies" -H 'Content-Type: application/json' -d @config/opensearch_mappings.json

index-delete: ## Delete OpenSearch indices
	@echo "$(YELLOW)Deleting OpenSearch indices...$(NC)"
	curl -X DELETE "localhost:9200/companies"

index-reindex: ## Reindex all data
	@echo "$(GREEN)Reindexing data...$(NC)"
	$(DOCKER_COMPOSE) exec api python -m src.search.reindex

# Testing commands
test: ## Run all tests
	@echo "$(GREEN)Running tests...$(NC)"
	$(DOCKER_COMPOSE) exec api pytest tests/ -v --cov=src --cov-report=html

test-unit: ## Run unit tests
	@echo "$(GREEN)Running unit tests...$(NC)"
	$(DOCKER_COMPOSE) exec api pytest tests/unit/ -v

test-integration: ## Run integration tests
	@echo "$(GREEN)Running integration tests...$(NC)"
	$(DOCKER_COMPOSE) exec api pytest tests/integration/ -v

test-compliance: ## Run compliance tests
	@echo "$(GREEN)Running GDPR/KVKK compliance tests...$(NC)"
	$(DOCKER_COMPOSE) exec api pytest tests/compliance/ -v

lint: ## Run code linting
	@echo "$(GREEN)Running linters...$(NC)"
	$(DOCKER_COMPOSE) exec api flake8 src/
	$(DOCKER_COMPOSE) exec api black --check src/
	$(DOCKER_COMPOSE) exec api mypy src/

format: ## Format code
	@echo "$(GREEN)Formatting code...$(NC)"
	$(DOCKER_COMPOSE) exec api black src/
	$(DOCKER_COMPOSE) exec api isort src/

# Monitoring commands
monitor-status: ## Check service status
	@echo "$(GREEN)Checking service status...$(NC)"
	@curl -s http://localhost:8000/api/v1/health | jq '.'
	@echo ""
	@curl -s http://localhost:9200/_cluster/health | jq '.'

monitor-metrics: ## View Prometheus metrics
	@echo "$(GREEN)Opening Prometheus metrics...$(NC)"
	@open http://localhost:9090 || xdg-open http://localhost:9090

monitor-dashboard: ## Open Grafana dashboard
	@echo "$(GREEN)Opening Grafana dashboard...$(NC)"
	@open http://localhost:3000 || xdg-open http://localhost:3000

# Production commands
prod: ## Deploy to production
	@echo "$(GREEN)Deploying to production...$(NC)"
	docker-compose -f docker-compose.prod.yml up -d

prod-update: ## Update production deployment
	@echo "$(GREEN)Updating production...$(NC)"
	docker-compose -f docker-compose.prod.yml pull
	docker-compose -f docker-compose.prod.yml up -d

# Cleanup commands
clean: ## Clean up containers, volumes, and cache
	@echo "$(RED)Cleaning up...$(NC)"
	$(DOCKER_COMPOSE) down -v
	rm -rf .cache/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Cleanup complete!$(NC)"

clean-data: ## Clean all collected data (CAREFUL!)
	@echo "$(RED)WARNING: This will delete all collected data!$(NC)"
	@read -p "Are you sure? (y/N) " confirm && [ "$$confirm" = "y" ] || exit 1
	$(DOCKER_COMPOSE) exec postgres psql -U marketing_user -d marketing_platform -c "TRUNCATE TABLE unified_companies CASCADE;"
	curl -X DELETE "localhost:9200/companies"
	@echo "$(GREEN)Data cleaned!$(NC)"

# Utility commands
serve: ## Start API server locally
	@echo "$(GREEN)Starting API server...$(NC)"
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

airflow-init: ## Initialize Airflow
	@echo "$(GREEN)Initializing Airflow...$(NC)"
	$(DOCKER_COMPOSE) exec airflow-scheduler airflow db init
	$(DOCKER_COMPOSE) exec airflow-scheduler airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

version: ## Show version information
	@echo "Marketing Data Platform v1.0.0"
	@$(DOCKER_COMPOSE) --version
	@$(PYTHON) --version

.DEFAULT_GOAL := help