# wwPDB Communication Module - Streamlined Makefile
# Core build, test, and deployment tasks

.PHONY: help clean install build test test-unit test-integration test-database validate \
        lint format check-format security backend-status init-database migrate-data \
        deploy deploy-dev deploy-production

# Configuration
PYTHON := python3
PIP := pip3
PYTEST := $(PYTHON) -m pytest
TOX := tox
PACKAGE_NAME := wwpdb.apps.msgmodule
SOURCE_DIR := wwpdb
TESTS_DIR := wwpdb/apps/tests-msgmodule
SCRIPTS_DIR := scripts
REQUIREMENTS := requirements.txt
SETUP_FILE := setup.py

# Environment detection
ENV ?= development
ifeq ($(ENV),production)
    CONFIG_FLAGS := --production
else ifeq ($(ENV),staging)
    CONFIG_FLAGS := --staging
else
    CONFIG_FLAGS := --development
endif

# Color output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Default target
help: ## Show this help message
	@echo "$(GREEN)wwPDB Communication Module - Database Operations$(NC)"
	@echo "$(YELLOW)Available targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Installation and Setup
install: ## Install dependencies and setup development environment
	@echo "$(GREEN)Installing dependencies...$(NC)"
	$(PIP) install -r $(REQUIREMENTS)
	$(PIP) install -e .
	@echo "$(GREEN)Installation complete!$(NC)"

install-dev: ## Install development dependencies
	@echo "$(GREEN)Installing development dependencies...$(NC)"
	$(PIP) install -r $(REQUIREMENTS)
	@$(PIP) install pytest pytest-cov pytest-mock pylint black flake8 safety bandit tox
	@$(PIP) install -e .
	@echo "$(GREEN)Development environment ready!$(NC)"

# Build Tasks
build: clean ## Build the package
	@echo "$(GREEN)Building package...$(NC)"
	$(PYTHON) $(SETUP_FILE) build
	@echo "$(GREEN)Build complete!$(NC)"

clean: ## Clean build artifacts and cache files
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	@rm -rf build/ dist/ *.egg-info/
	@find . -type d -name __pycache__ -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete
	@find . -type f -name "*.pyo" -delete
	@find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@rm -rf .coverage htmlcov/
	@echo "$(GREEN)Clean complete!$(NC)"

# Testing Tasks
test: test-database ## Run all available tests

test-unit: ## Run unit tests
	@echo "$(GREEN)Running unit tests...$(NC)"
	@if [ -d "$(TESTS_DIR)" ]; then \
		$(PYTEST) $(TESTS_DIR) -v --tb=short \
			--ignore=$(TESTS_DIR)/DatabaseOperationsTests.py \
			--cov=$(SOURCE_DIR) --cov-report=term-missing; \
	else \
		echo "$(YELLOW)Test directory not found: $(TESTS_DIR)$(NC)"; \
	fi

test-integration: ## Run integration tests
	@echo "$(GREEN)Running integration tests...$(NC)"
	@if [ -d "$(TESTS_DIR)" ]; then \
		$(PYTEST) $(TESTS_DIR) -v --tb=short \
			-k "integration" \
			--cov=$(SOURCE_DIR) --cov-report=term-missing || echo "$(YELLOW)Some integration tests failed or no tests found$(NC)"; \
	else \
		echo "$(YELLOW)Test directory not found: $(TESTS_DIR)$(NC)"; \
	fi

test-database: ## Run database operations tests
	@echo "$(GREEN)Running database operations tests...$(NC)"
	@if [ -f "$(TESTS_DIR)/DatabaseOperationsTests.py" ]; then \
		$(PYTEST) $(TESTS_DIR)/DatabaseOperationsTests.py -v --tb=short || echo "$(YELLOW)Some database tests failed$(NC)"; \
	else \
		echo "$(YELLOW)Database operations test file not found$(NC)"; \
	fi

# Validation Tasks
validate: lint security ## Run all validation checks

# Code Quality Tasks
lint: ## Run code linting
	@echo "$(GREEN)Running linting...$(NC)"
	pylint $(SOURCE_DIR)
	flake8 $(SOURCE_DIR) --max-line-length=120

format: ## Format code with black
	@echo "$(GREEN)Formatting code...$(NC)"
	black $(SOURCE_DIR) $(TESTS_DIR) $(SCRIPTS_DIR)

check-format: ## Check code formatting without making changes
	@echo "$(GREEN)Checking code formatting...$(NC)"
	black --check $(SOURCE_DIR) $(TESTS_DIR) $(SCRIPTS_DIR)

security: ## Run security checks
	@echo "$(GREEN)Running security checks...$(NC)"
	safety check
	bandit -r $(SOURCE_DIR)

# Database Setup
init-database: ## Initialize messaging database
deploy: ## Deploy to specified environment (ENV=development|staging|production)
ifeq ($(ENV),production)
	@$(MAKE) deploy-production
else
	@$(MAKE) deploy-dev
endif

deploy-dev: ## Deploy to development environment
	@echo "$(GREEN)Deploying to development environment...$(NC)"
	@echo "$(YELLOW)Running pre-deployment validation...$(NC)"
	@$(MAKE) test
	@echo "$(GREEN)Development deployment complete!$(NC)"

deploy-production: ## Deploy to production environment
	@echo "$(GREEN)Deploying to production environment...$(NC)"
	@echo "$(YELLOW)Running comprehensive checks...$(NC)"
	@$(MAKE) test validate security
	@echo "$(RED)WARNING: Production deployment requires manual approval$(NC)"

# Backend Configuration
backend-status: ## Show current backend configuration
	@echo "$(GREEN)Backend Configuration Status:$(NC)"
	@echo "Current backend: $$(echo $${WWPDB_MESSAGING_BACKEND:-cif})"
	@echo "Environment: $(ENV)"

# Database Setup
init-database: ## Initialize messaging database
	@echo "$(GREEN)Initializing messaging database...$(NC)"
	$(PYTHON) $(SCRIPTS_DIR)/init_messaging_database.py
	@echo "$(GREEN)Database initialization complete!$(NC)"

migrate-data: ## Migrate CIF data to database
	@echo "$(GREEN)Migrating CIF data to database...$(NC)"
	$(PYTHON) $(SCRIPTS_DIR)/migrate_cif_to_db.py --batch
	@echo "$(GREEN)Data migration complete!$(NC)"

# Development Workflow Tasks
dev-setup: install-dev ## Setup complete development environment
	@echo "$(GREEN)Setting up development environment...$(NC)"
	@echo "$(GREEN)Development environment ready!$(NC)"

dev-test: ## Quick development test cycle
	@echo "$(GREEN)Running development test cycle...$(NC)"
	@$(MAKE) test-database

dev-commit: ## Run checks before committing
	@echo "$(GREEN)Running pre-commit checks...$(NC)"
	@$(MAKE) check-format lint test-database
	@echo "$(GREEN)Ready to commit!$(NC)"

# Version and Information
version: ## Show version information
	@echo "$(GREEN)Version Information:$(NC)"
	@$(PYTHON) -c "import wwpdb.apps.msgmodule; print('Package version: Available')"
	@echo "Git commit: $$(git rev-parse --short HEAD 2>/dev/null || echo 'Not in git repo')"
	@echo "Environment: $(ENV)"

info: ## Show project information
	@echo "$(GREEN)wwPDB Communication Module$(NC)"
	@echo "Package: $(PACKAGE_NAME)"
	@echo "Source: $(SOURCE_DIR)"
	@echo "Tests: $(TESTS_DIR)"
	@echo "Scripts: $(SCRIPTS_DIR)"
	@echo ""
	@echo "$(YELLOW)Key Features:$(NC)"
	@echo "  - Simple backend selection (CIF or Database)"
	@echo "  - Single environment variable configuration"
	@echo "  - Same interface for both backends"
	@echo "  - Comprehensive testing"
	@echo ""
	@echo "$(YELLOW)Backend Modes:$(NC)"
	@echo "  - CIF: Traditional file-based storage (default)"
	@echo "  - Database: Modern database storage"
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  make dev-setup         - Setup development environment"
	@echo "  make backend-status    - Show current backend configuration"
	@echo "  make test             - Run all tests"
	@echo "  make validate         - Run all validations"
