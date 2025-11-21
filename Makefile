# Makefile for target-redshift
# This Makefile handles special characters automatically across different shells (bash, zsh, etc.)
# Note: Zsh users can also run these commands directly without using make

.PHONY: help venv install install-dev test test-unit test-integration test-mock-integration docker-up docker-down docker-logs lint clean coverage

help:
	@echo "Available targets:"
	@echo ""
	@echo "Setup:"
	@echo "  venv              - Create a virtual environment"
	@echo "  install           - Install package dependencies"
	@echo "  install-dev       - Install package with development dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  test              - Run all tests (unit + mock integration with Docker)"
	@echo "  test-unit         - Run unit tests with coverage"
	@echo "  test-integration  - Run integration tests (requires Redshift and S3)"
	@echo "  test-mock-integration - Run mock integration tests using Docker PostgreSQL"
	@echo ""
	@echo "Docker (for mock integration tests):"
	@echo "  docker-up         - Start PostgreSQL container for testing"
	@echo "  docker-down       - Stop and remove PostgreSQL container"
	@echo "  docker-logs       - Show PostgreSQL container logs"
	@echo ""
	@echo "Code quality:"
	@echo "  lint              - Run pylint on the codebase"
	@echo "  coverage          - Generate coverage report"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean             - Remove virtual environment and build artifacts"
	@echo "  clean-docker      - Remove Docker containers, volumes, and images"
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

test: docker-up
	@echo "=========================================="
	@echo "Running all tests (unit + mock integration)"
	@echo "=========================================="
	@echo ""
	bash -c "source .venv/bin/activate && coverage run -m pytest -vv --disable-pytest-warnings tests/unit tests/mock_integration && coverage report"

test-unit:
	bash -c "source .venv/bin/activate && coverage run -m pytest -vv --disable-pytest-warnings tests/unit && coverage report"

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
	bash -c "source .venv/bin/activate && coverage run -m pytest -vv --disable-pytest-warnings tests/integration && coverage report"

test-mock-integration: docker-up
	@echo "=========================================="
	@echo "Running mock integration tests with Docker PostgreSQL"
	@echo "No AWS credentials required!"
	@echo "=========================================="
	@echo ""
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5
	bash -c "source .venv/bin/activate && coverage run -m pytest -vv --disable-pytest-warnings tests/mock_integration && coverage report"

docker-up:
	@echo "Starting PostgreSQL container for mock integration tests..."
	docker-compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5
	@docker-compose ps

docker-down:
	@echo "Stopping PostgreSQL container..."
	docker-compose down -v

docker-logs:
	docker-compose logs -f postgres

lint:
	bash -c "source .venv/bin/activate && pylint target_redshift -d C,W,R,unexpected-keyword-arg,duplicate-code"

coverage:
	bash -c "source .venv/bin/activate && coverage report && coverage html"
	@echo "HTML coverage report generated in htmlcov/index.html"

clean:
	rm -rf .venv
	rm -rf build dist *.egg-info
	rm -rf .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-docker: docker-down
	@echo "Removing Docker volumes and images..."
	docker-compose down -v --rmi local
