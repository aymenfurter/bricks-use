.PHONY: test test-unit test-integration lint format clean install dev-install

# Test commands
test:
	pytest

test-unit:
	pytest -m "not integration"

test-integration:
	pytest -m integration

test-coverage:
	pytest --cov=src --cov-report=html --cov-report=term

# Code quality
lint:
	flake8 src/
	mypy src/

format:
	black src/
	isort src/

# Development setup
install:
	pip install -r requirements.txt

dev-install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

# Cleanup
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .coverage htmlcov/ .pytest_cache/

# Run server
run:
	python databricks_server.py

# CLI commands
cli-query:
	python cli.py query "$(QUERY)"

cli-info:
	python cli.py info "$(TABLE)"

cli-compare:
	python cli.py compare "$(TABLE1)" "$(TABLE2)"

# Make CLI executable
setup-cli:
	chmod +x cli.py
	chmod +x bricks

# Help
help:
	@echo "Available commands:"
	@echo "  test           - Run all tests"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  lint           - Run linting (flake8, mypy)"
	@echo "  format         - Format code (black, isort)"
	@echo "  install        - Install production dependencies"
	@echo "  dev-install    - Install development dependencies"
	@echo "  clean          - Clean up temporary files"
	@echo "  run            - Run the server"
	@echo "  setup-cli      - Make CLI executable"
	@echo "  cli-query      - Run query via CLI (set QUERY variable)"
	@echo "  cli-info       - Get table info via CLI (set TABLE variable)"
	@echo "  cli-compare    - Compare tables via CLI (set TABLE1 and TABLE2 variables)"
	@echo ""
	@echo "CLI Wrapper Usage:"
	@echo "  ./bricks query \"SELECT * FROM table\""
	@echo "  ./bricks info table_name"
	@echo "  ./bricks compare table1 table2"
