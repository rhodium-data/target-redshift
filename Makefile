# Makefile for pipelinewise-target-redshift
# This Makefile handles special characters automatically across different shells (bash, zsh, etc.)
# Note: Zsh users can also run these commands directly without using make

.PHONY: help venv install install-dev test test-unit test-integration lint clean coverage

help:
	@echo "Available targets:"
	@echo "  venv              - Create a virtual environment"
	@echo "  install           - Install package dependencies"
	@echo "  install-dev       - Install package with development dependencies"
	@echo "  test              - Run all tests (unit tests only, use test-integration for integration)"
	@echo "  test-unit         - Run unit tests with coverage"
	@echo "  test-integration  - Run integration tests (requires environment variables)"
	@echo "  lint              - Run pylint on the codebase"
	@echo "  coverage          - Generate coverage report"
	@echo "  clean             - Remove virtual environment and build artifacts"
	@echo ""
	@echo "Platform notes:"
	@echo "  - The Makefile works on all platforms (bash, zsh, fish, etc.)"
	@echo "  - If running pip commands manually in zsh, escape brackets: pip install .\[test\]"

venv:
	python3 -m venv .venv
	@echo "Virtual environment created. Activate it with: source .venv/bin/activate"

install:
	pip install --upgrade pip
	pip install .

# Note: In zsh, .[test] requires escaping as .\[test\] when run manually,
# but make handles this automatically
install-dev:
	pip install --upgrade pip
	pip install .[test]
	pip install pylint

test: test-unit

test-unit:
	coverage run -m pytest -vv --disable-pytest-warnings tests/unit
	coverage report

test-integration:
	@echo "=========================================="
	@echo "WARNING: Integration tests require:"
	@echo "  - Live Redshift cluster"
	@echo "  - S3 bucket for staging"
	@echo "  - Tests will DROP and recreate the target schema"
	@echo "  - Never use production credentials!"
	@echo "=========================================="
	@echo ""
	@echo "Required environment variables:"
	@echo "  TARGET_REDSHIFT_HOST"
	@echo "  TARGET_REDSHIFT_PORT"
	@echo "  TARGET_REDSHIFT_USER"
	@echo "  TARGET_REDSHIFT_PASSWORD"
	@echo "  TARGET_REDSHIFT_DBNAME"
	@echo "  TARGET_REDSHIFT_SCHEMA (will be dropped!)"
	@echo "  TARGET_REDSHIFT_AWS_ACCESS_KEY"
	@echo "  TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY"
	@echo "  TARGET_REDSHIFT_S3_ACL"
	@echo "  TARGET_REDSHIFT_S3_BUCKET"
	@echo "  TARGET_REDSHIFT_S3_KEY_PREFIX"
	@echo ""
	@echo "See README.md Testing section for detailed setup instructions."
	@echo ""
	coverage run -m pytest -vv --disable-pytest-warnings tests/integration
	coverage report

lint:
	pylint target_redshift -d C,W,unexpected-keyword-arg,duplicate-code

coverage:
	coverage report
	coverage html
	@echo "HTML coverage report generated in htmlcov/index.html"

clean:
	rm -rf .venv
	rm -rf build dist *.egg-info
	rm -rf .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
