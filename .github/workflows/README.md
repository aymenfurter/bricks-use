# CI/CD Pipeline Documentation

This directory contains the GitHub Actions workflows for the Databricks MCP Server project.

## Workflows

### CI/CD Pipeline (`ci.yml`)

A comprehensive continuous integration and deployment pipeline that runs on:
- Push to `main` branch
- Pull requests to `main` branch  
- Manual dispatch

#### Jobs

##### 1. Code Quality & Linting (`lint`)
- **Runtime**: Ubuntu Latest with Python 3.11
- **Checks**:
  - Critical syntax errors with flake8 (fails build if found)
  - All code style issues with flake8 (informational)
  - Type checking with mypy (informational)
  - Code formatting with black (informational)
  - Import sorting with isort (informational)

##### 2. Unit Tests (`test`)
- **Runtime**: Ubuntu Latest with Python 3.11 and 3.12
- **Features**:
  - Runs full test suite (pytest)
  - Generates coverage reports
  - Tests Makefile targets
  - Uploads coverage to Codecov (Python 3.11 only)
  - Uses dependency caching for faster builds

#### Key Features

- **Fast builds**: pip dependency caching
- **Multi-version testing**: Python 3.11 and 3.12 support
- **Comprehensive quality checks**: Linting, formatting, type checking
- **Coverage reporting**: Integrated with Codecov
- **Flexible failure policy**: Critical errors fail, style issues are informational
- **Production ready**: Following GitHub Actions best practices

#### Badge

The pipeline status is displayed in the README.md with a badge:
```markdown
[![CI/CD Pipeline](https://github.com/aymenfurter/bricks-use/actions/workflows/ci.yml/badge.svg)](https://github.com/aymenfurter/bricks-use/actions/workflows/ci.yml)
```

#### Local Testing

You can simulate the CI pipeline locally:

```bash
# Install dependencies
make dev-install

# Run critical checks (what CI requires to pass)
flake8 src/ --count --select=E9,F63,F7,F82 --show-source --statistics

# Run tests
make test

# Run coverage
pytest --cov=src --cov-report=term
```