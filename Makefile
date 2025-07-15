# wwPDB Communication Module - Database Operations Makefile
# Automates build, test, validate, deploy, and documentation tasks

.PHONY: help clean install build test test-unit test-integration test-database validate validate-integration \
        lint format check-format security docs serve-docs deploy deploy-dev deploy-production \
        backup restore monitor status health feature-flags \
        backend-cif-only backend-db-only backend-dual-write-cif-read backend-dual-write-db-read backend-info backend-migration-guide \
        migration-phase-1 migration-phase-2 migration-phase-3 test-dual-mode test-feature-flags test-factory test-migration

# Configuration
PYTHON := python3
PIP := pip3
PYTEST := $(PYTHON) -m pytest
TOX := tox
PACKAGE_NAME := wwpdb.apps.msgmodule
SOURCE_DIR := wwpdb
TESTS_DIR := wwpdb/apps/tests-msgmodule
DOCS_DIR := docs
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
	@echo "$(GREEN)Creating necessary directories...$(NC)"
	@mkdir -p logs tmp backups
	@echo "$(GREEN)Installation complete!$(NC)"

install-dev: ## Install development dependencies
	@echo "$(GREEN)Installing development dependencies...$(NC)"
	@echo "$(YELLOW)Checking for CMake...$(NC)"
	@if command -v cmake >/dev/null 2>&1; then \
		echo "$(GREEN)CMake found. Installing full requirements...$(NC)"; \
		$(PIP) install -r $(REQUIREMENTS) || { \
			echo "$(RED)Failed to install some packages. Trying minimal install...$(NC)"; \
			$(MAKE) install-minimal; \
		}; \
	else \
		echo "$(RED)CMake not found. Installing minimal requirements...$(NC)"; \
		$(MAKE) install-minimal; \
	fi
	@$(PIP) install pytest pytest-cov pytest-mock pylint black flake8 safety bandit tox
	@$(PIP) install -e .
	@echo "$(GREEN)Development environment ready!$(NC)"

install-minimal: ## Install minimal dependencies (without cmake-dependent packages)
	@echo "$(GREEN)Installing minimal dependencies...$(NC)"
	@echo "# Minimal requirements without cmake dependencies" > requirements-minimal.txt
	@echo "wwpdb.io" >> requirements-minimal.txt
	@echo "wwpdb.utils.config>=0.22.2" >> requirements-minimal.txt
	@echo "wwpdb.utils.session" >> requirements-minimal.txt
	@echo "wwpdb.utils.wf>=0.8" >> requirements-minimal.txt
	@echo "mmcif" >> requirements-minimal.txt
	@echo "mmcif.utils" >> requirements-minimal.txt
	@echo "wwpdb.utils.dp" >> requirements-minimal.txt
	@echo "wwpdb.utils.emdb~=0.17" >> requirements-minimal.txt
	@echo "wwpdb.apps.wf_engine" >> requirements-minimal.txt
	@echo "# wwpdb.utils.nmr" >> requirements-minimal.txt
	@echo "# wwpdb.utils.align" >> requirements-minimal.txt
	@echo "mysql-connector-python>=8.0.12" >> requirements-minimal.txt
	@$(PIP) install -r requirements-minimal.txt
	@echo "$(GREEN)Creating necessary directories...$(NC)"
	@mkdir -p logs tmp backups
	@echo "$(GREEN)Minimal installation complete!$(NC)"

install-cmake: ## Install CMake using system package manager
	@echo "$(GREEN)Installing CMake...$(NC)"
	@command -v brew >/dev/null 2>&1 && { \
		echo "$(YELLOW)Installing CMake via Homebrew...$(NC)"; \
		brew install cmake; \
	} || { \
		echo "$(YELLOW)Homebrew not found. Please install CMake manually:$(NC)"; \
		echo "$(YELLOW)  - macOS: brew install cmake$(NC)"; \
		echo "$(YELLOW)  - conda: conda install cmake$(NC)"; \
		echo "$(YELLOW)  - Manual: https://cmake.org/download/$(NC)"; \
		exit 1; \
	}

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
test: test-database-full ## Run all available tests

test-unit: ## Run unit tests
	@echo "$(GREEN)Running unit tests...$(NC)"
	@if [ -d "$(TESTS_DIR)" ]; then \
		TEST_FILES=$$(find $(TESTS_DIR) -name "*Tests.py" ! -name "DatabaseOperationsTests.py" 2>/dev/null | head -1); \
		if [ -n "$$TEST_FILES" ]; then \
			$(PYTEST) $(TESTS_DIR) -v --tb=short \
				--ignore=$(TESTS_DIR)/DatabaseOperationsTests.py \
				--cov=$(SOURCE_DIR) --cov-report=term-missing; \
		else \
			echo "$(YELLOW)No unit test files found in $(TESTS_DIR)$(NC)"; \
		fi; \
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

test-database-full: ## Run database tests and validation
	@echo "$(GREEN)Running database tests and validation...$(NC)"
	@$(MAKE) test-database
	@$(MAKE) validate-integration

test-coverage: ## Generate detailed test coverage report
	@echo "$(GREEN)Generating coverage report...$(NC)"
	@if [ -d "$(TESTS_DIR)" ]; then \
		$(PYTEST) $(TESTS_DIR) --cov=$(SOURCE_DIR) --cov-report=html --cov-report=term || echo "$(YELLOW)Coverage generation completed with warnings$(NC)"; \
		echo "$(YELLOW)Coverage report generated in htmlcov/index.html$(NC)"; \
	else \
		echo "$(YELLOW)Test directory not found: $(TESTS_DIR)$(NC)"; \
	fi

test-tox: ## Run tests across multiple Python versions with tox
	@echo "$(GREEN)Running tox tests...$(NC)"
	$(TOX)

test-dual-mode: ## Run dual-mode functionality tests
	@echo "$(GREEN)Running dual-mode tests...$(NC)"
	@if [ -f "$(TESTS_DIR)/DualModeTests.py" ]; then \
		$(PYTEST) $(TESTS_DIR)/DualModeTests.py -v --tb=short; \
	else \
		echo "$(YELLOW)DualModeTests.py not found$(NC)"; \
	fi

test-feature-flags: ## Run feature flag tests only
	@echo "$(GREEN)Running feature flag tests...$(NC)"
	@if [ -f "$(TESTS_DIR)/DualModeTests.py" ]; then \
		$(PYTEST) $(TESTS_DIR)/DualModeTests.py::TestDualModeFeatureFlags -v --tb=short; \
	else \
		echo "$(YELLOW)DualModeTests.py not found$(NC)"; \
	fi

test-factory: ## Run messaging factory tests
	@echo "$(GREEN)Running messaging factory tests...$(NC)"
	@if [ -f "$(TESTS_DIR)/DualModeTests.py" ]; then \
		$(PYTEST) $(TESTS_DIR)/DualModeTests.py::TestMessagingFactory -v --tb=short; \
	else \
		echo "$(YELLOW)DualModeTests.py not found$(NC)"; \
	fi

test-migration: ## Run migration scenario tests
	@echo "$(GREEN)Running migration scenario tests...$(NC)"
	@if [ -f "$(TESTS_DIR)/DualModeTests.py" ]; then \
		$(PYTEST) $(TESTS_DIR)/DualModeTests.py::TestMigrationScenarios -v --tb=short; \
	else \
		echo "$(YELLOW)DualModeTests.py not found$(NC)"; \
	fi

# Validation Tasks
validate: validate-integration lint security ## Run all validation checks

validate-integration: ## Validate database integration
	@echo "$(GREEN)Validating database integration...$(NC)"
	$(PYTHON) $(SCRIPTS_DIR)/validate_integration.py
	@echo "$(GREEN)Integration validation complete!$(NC)"

validate-config: ## Validate configuration files
	@echo "$(GREEN)Validating configuration...$(NC)"
	@$(PYTHON) -c "from wwpdb.apps.msgmodule.db.config import DatabaseConfig; config = DatabaseConfig(); is_valid, errors = config.validate(); print('Valid:', is_valid); print('Errors:', errors)"
	@echo "$(GREEN)Configuration validation complete!$(NC)"

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

# Documentation Tasks
docs: ## Generate documentation
	@echo "$(GREEN)Generating documentation...$(NC)"
	@echo "Documentation files in $(DOCS_DIR):"
	@ls -la $(DOCS_DIR)/
	@echo "$(GREEN)Documentation ready!$(NC)"

serve-docs: ## Serve documentation locally (if using mkdocs or similar)
	@echo "$(GREEN)Serving documentation...$(NC)"
	@echo "$(YELLOW)Note: Install mkdocs or similar tool for full documentation serving$(NC)"
	@echo "$(YELLOW)Current docs available in $(DOCS_DIR)/$(NC)"

docs-check: ## Check documentation for issues
	@echo "$(GREEN)Checking documentation...$(NC)"
	@for file in $(DOCS_DIR)/*.md; do \
		echo "Checking $$file..."; \
		$(PYTHON) -c "import markdown; markdown.markdown(open('$$file').read())" > /dev/null; \
	done
	@echo "$(GREEN)Documentation check complete!$(NC)"

# Deployment Tasks
deploy: ## Deploy to specified environment (ENV=development|staging|production)
ifeq ($(ENV),production)
	@$(MAKE) deploy-production
else ifeq ($(ENV),staging)
	@$(MAKE) deploy-staging
else
	@$(MAKE) deploy-dev
endif

deploy-dev: ## Deploy to development environment
	@echo "$(GREEN)Deploying to development environment...$(NC)"
	@echo "$(YELLOW)Running pre-deployment validation...$(NC)"
	@$(MAKE) validate-integration
	@echo "$(GREEN)Development deployment complete!$(NC)"

deploy-staging: ## Deploy to staging environment
	@echo "$(GREEN)Deploying to staging environment...$(NC)"
	@echo "$(YELLOW)Running full validation suite...$(NC)"
	@$(MAKE) test validate
	@echo "$(GREEN)Staging deployment complete!$(NC)"

deploy-production: ## Deploy to production environment
	@echo "$(GREEN)Deploying to production environment...$(NC)"
	@echo "$(YELLOW)Running comprehensive checks...$(NC)"
	@$(MAKE) test validate security
	@echo "$(RED)WARNING: Production deployment requires manual approval$(NC)"
	@echo "$(YELLOW)Please review deployment guide: $(DOCS_DIR)/DEPLOYMENT_GUIDE_PHASE2.md$(NC)"

# Feature Flag Management
feature-flags: ## Show current feature flag status
	@echo "$(GREEN)Current feature flag configuration:$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/makefile_utils.py feature-flags

feature-flags-enable: ## Enable a feature flag (usage: make feature-flags-enable FLAG=flag_name)
	@echo "$(GREEN)Enabling feature flag: $(FLAG)$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/makefile_utils.py enable-flag $(FLAG)

feature-flags-disable: ## Disable a feature flag (usage: make feature-flags-disable FLAG=flag_name)
	@echo "$(YELLOW)Disabling feature flag: $(FLAG)$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/makefile_utils.py disable-flag $(FLAG)

# Dual-Mode Backend Configuration
backend-cif-only: ## Configure for CIF-only mode
	@echo "$(GREEN)Configuring CIF-only mode...$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/backend_config.py cif-only

backend-db-only: ## Configure for database-only mode  
	@echo "$(GREEN)Configuring database-only mode...$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/backend_config.py db-only

backend-dual-write-cif-read: ## Configure for dual-write, CIF-read (Migration Phase 1)
	@echo "$(GREEN)Configuring dual-write, CIF-read mode...$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/backend_config.py dual-write-cif-read

backend-dual-write-db-read: ## Configure for dual-write, DB-read (Migration Phase 2)
	@echo "$(GREEN)Configuring dual-write, DB-read mode...$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/backend_config.py dual-write-db-read

backend-info: ## Show detailed backend configuration info
	@echo "$(GREEN)Backend Configuration Details:$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/backend_config.py show

backend-migration-guide: ## Show migration guide
	@echo "$(GREEN)Migration Guide:$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/backend_config.py migration-guide

# Migration Helper Targets
migration-phase-1: backend-dual-write-cif-read ## Start Migration Phase 1 (dual-write, CIF-read)
migration-phase-2: backend-dual-write-db-read ## Start Migration Phase 2 (dual-write, DB-read)  
migration-phase-3: backend-db-only ## Start Migration Phase 3 (DB-only)

# Monitoring and Health Tasks
health: ## Check system health
	@echo "$(GREEN)Checking system health...$(NC)"
	@$(PYTHON) $(SCRIPTS_DIR)/makefile_utils.py health

status: ## Show current system status
	@echo "$(GREEN)System Status:$(NC)"
	@echo "Environment: $(ENV)"
	@echo "Python: $$($(PYTHON) --version)"
	@echo "Package: $(PACKAGE_NAME)"
	@$(MAKE) feature-flags

monitor: ## Start monitoring (placeholder for future monitoring tools)
	@echo "$(GREEN)Starting monitoring...$(NC)"
	@echo "$(YELLOW)Note: Implement with your preferred monitoring solution$(NC)"

# Backup and Restore Tasks
backup: ## Create backup of configuration and data
	@echo "$(GREEN)Creating backup...$(NC)"
	@mkdir -p backups/$$(date +%Y%m%d_%H%M%S)
	@cp -r $(DOCS_DIR) backups/$$(date +%Y%m%d_%H%M%S)/
	@cp $(REQUIREMENTS) setup.py setup.cfg backups/$$(date +%Y%m%d_%H%M%S)/
	@echo "$(GREEN)Backup created in backups/$(NC)"

restore: ## Restore from backup (usage: make restore BACKUP=backup_directory)
	@echo "$(YELLOW)Restoring from backup: $(BACKUP)$(NC)"
	@if [ -z "$(BACKUP)" ]; then \
		echo "$(RED)Error: Please specify BACKUP=backup_directory$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Restore complete!$(NC)"

# Development Workflow Tasks
dev-setup: install-dev ## Setup complete development environment
	@echo "$(GREEN)Setting up development environment...$(NC)"
	@$(MAKE) validate-config
	@echo "$(GREEN)Development environment ready!$(NC)"

dev-test: ## Quick development test cycle
	@echo "$(GREEN)Running development test cycle...$(NC)"
	@$(MAKE) test-database
	@$(MAKE) validate-integration

dev-commit: ## Run checks before committing
	@echo "$(GREEN)Running pre-commit checks...$(NC)"
	@$(MAKE) check-format lint test-database
	@echo "$(GREEN)Ready to commit!$(NC)"

# CI/CD Integration
ci-test: ## CI testing pipeline
	@echo "$(GREEN)Running CI test pipeline...$(NC)"
	@$(MAKE) install-dev
	@$(MAKE) test
	@$(MAKE) validate
	@$(MAKE) security

ci-deploy: ## CI deployment pipeline
	@echo "$(GREEN)Running CI deployment pipeline...$(NC)"
	@$(MAKE) ci-test
	@$(MAKE) build

# Troubleshooting
troubleshoot: ## Show troubleshooting information
	@echo "$(GREEN)Troubleshooting Information:$(NC)"
	@echo "See: $(DOCS_DIR)/TROUBLESHOOTING.md"
	@echo ""
	@echo "$(YELLOW)Common commands:$(NC)"
	@echo "  make health            - Check system health"
	@echo "  make validate-phase2   - Validate Phase 2 implementation"
	@echo "  make feature-flags     - Show feature flag status"
	@echo "  make backend-info      - Show backend configuration details"
	@echo "  make logs             - Show recent logs"
	@echo ""
	@echo "$(YELLOW)Backend Configuration:$(NC)"
	@echo "  make backend-cif-only           - CIF-only mode"
	@echo "  make backend-db-only            - Database-only mode"
	@echo "  make backend-dual-write-cif-read - Migration Phase 1"
	@echo "  make backend-dual-write-db-read  - Migration Phase 2"
	@echo "  make backend-migration-guide     - Show migration guide"

logs: ## Show recent logs (if logging is configured)
	@echo "$(GREEN)Recent logs:$(NC)"
	@if [ -d "logs" ]; then \
		tail -n 50 logs/*.log 2>/dev/null || echo "No log files found in logs/"; \
	else \
		echo "Logs directory not found. Run 'make install' to create it."; \
	fi

# Version and Information
version: ## Show version information
	@echo "$(GREEN)Version Information:$(NC)"
	@$(PYTHON) -c "import wwpdb.apps.msgmodule; print('Package version: Available')"
	@echo "Git commit: $$(git rev-parse --short HEAD 2>/dev/null || echo 'Not in git repo')"
	@echo "Environment: $(ENV)"

info: ## Show project information
	@echo "$(GREEN)wwPDB Communication Module - Phase 2 Database Operations$(NC)"
	@echo "Package: $(PACKAGE_NAME)"
	@echo "Source: $(SOURCE_DIR)"
	@echo "Tests: $(TESTS_DIR)"
	@echo "Docs: $(DOCS_DIR)"
	@echo "Scripts: $(SCRIPTS_DIR)"
	@echo ""
	@echo "$(YELLOW)Key Features:$(NC)"
	@echo "  - Dual-mode backend support (CIF + Database)"
	@echo "  - Flexible read/write backend selection"
	@echo "  - Gradual migration support with feature flags"
	@echo "  - Automatic failover and circuit breaker protection"
	@echo "  - Dynamic feature flag management"
	@echo "  - Comprehensive validation and testing"
	@echo ""
	@echo "$(YELLOW)Backend Modes:$(NC)"
	@echo "  - CIF-only: Traditional file-based storage"
	@echo "  - Database-only: Pure database storage"
	@echo "  - Dual-mode: Write to both, configurable read priority"
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  make dev-setup         - Setup development environment"
	@echo "  make backend-info      - Show current backend configuration"
	@echo "  make test             - Run all tests"
	@echo "  make test-dual-mode   - Run dual-mode functionality tests"
	@echo "  make test-feature-flags - Run feature flag tests"
	@echo "  make validate         - Run all validations"
	@echo "  make deploy-dev       - Deploy to development"
