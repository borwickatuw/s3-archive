SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'

# =============================================================================
# Setup
# =============================================================================

.PHONY: install
install: ## Install all dependencies (incl. dev + test)
	@uv sync

# =============================================================================
# Development
# =============================================================================

.PHONY: format
format: ## Apply code formatting with ruff
	@uv run ruff format .

.PHONY: lint
lint: ## Check linting and formatting with ruff
	@uv run ruff check .
	@uv run ruff format --check .

.PHONY: test
test: ## Run tests
	@uv run pytest

.PHONY: test-cov
test-cov: ## Run tests with coverage
	@uv run pytest --cov --cov-report=term-missing

.PHONY: check
check: lint test security ## Run lint + test + security

.PHONY: clean
clean: ## Remove build artifacts
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .coverage htmlcov dist build

# =============================================================================
# Security
# =============================================================================

.PHONY: security
security: security-bandit security-deps ## Run security checks
	@echo "=== Security Checks Complete ==="

.PHONY: security-bandit
security-bandit: ## Run bandit security linter
	@uv run bandit -c pyproject.toml -r src/ -ll

.PHONY: security-deps
security-deps: ## Check dependency vulnerabilities
	@uv run pip-audit

.PHONY: security-update
security-update: ## Update security-sensitive deps
	@uv lock --upgrade-package boto3 --upgrade-package botocore --upgrade-package urllib3 --upgrade-package certifi
	@uv sync

# =============================================================================
# CLI passthroughs (handy for local testing)
# =============================================================================

.PHONY: run
run: ## Invoke s3-archive with ARGS (e.g. make run ARGS='extract s3://... s3://...')
	@uv run s3-archive $(ARGS)
