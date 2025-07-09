<div align="center">

<img src="https://raw.githubusercontent.com/aymenfurter/bricks-use/refs/heads/main/bricks-use.png" alt="Project Logo" width="820" />

[![CI/CD Pipeline](https://img.shields.io/github/actions/workflow/status/aymenfurter/bricks-use/ci.yml?label=CI/CD&style=flat-square)](https://github.com/aymenfurter/bricks-use/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg?style=flat-square)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![Type Checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg?style=flat-square)](https://mypy.readthedocs.io/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-red.svg?style=flat-square)](https://databricks.com/)


**A powerful Model Context Protocol (MCP) server for executing Databricks SQL queries and comparing table data.**

*⚠️ This project is purely meant for demo purposes - use at your own risk!*

</div>

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Setup](#setup)
- [CLI Usage](#cli-usage)
- [MCP Tools](#mcp-tools)
- [VS Code Integration](#vs-code-integration)
- [License](#license)

## Features

| Feature | Description |
|---------|-------------|
| **Execute SQL Queries** | Run any SQL query on Databricks with configurable result limits |
| **Table Information** | Get detailed information about tables including schema and row counts |
| **Table Comparison** | Compare two tables by downloading their data and running CLI diff |
| **Quick Comparison** | Fast metadata-only comparison of tables |

## Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/aymenfurter/bricks-use.git
cd bricks-use
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment (see setup section)
cp .env.example .env  # Edit with your credentials

# 4. Run the server or use CLI
python databricks_server.py  # For MCP server
# OR
./bricks query "SELECT * FROM my_table LIMIT 10"  # For CLI
```

## Setup

### Prerequisites

<table>
<tr>
<td><strong>Python</strong></td>
<td>3.11 or higher</td>
</tr>
<tr>
<td><strong>Databricks</strong></td>
<td>Workspace access</td>
</tr>
<tr>
<td><strong>Token</strong></td>
<td>Personal access token</td>
</tr>
</table>

### Environment Variables

Set the following environment variables or create a `.env` file:

```bash
# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-personal-access-token

# Optional Settings
DATABRICKS_CATALOG=main                    # Defaults to 'main'
DATABRICKS_SCHEMA=default                  # Defaults to 'default'
DATABRICKS_TEMP_DIR=/tmp/databricks_mcp    # Temp directory
```

### Installation

1. **Create and activate a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## CLI Usage

Use the `./bricks` command-line tool for direct interaction:

```bash
# Execute SQL queries
./bricks query "SELECT * FROM my_table LIMIT 10"
./bricks query "SELECT COUNT(*) FROM users WHERE active = true" --limit 50

# Get table information
./bricks info my_table
./bricks info users --catalog production --schema analytics

# Compare tables
./bricks compare table1 table2
./bricks compare old_users new_users --quick
./bricks compare sales_2023 sales_2024 --catalog1 prod --schema1 sales

# Output options
./bricks query "SELECT * FROM table" --format json
./bricks info my_table --format table
```

## MCP Tools

<div align="center">

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `execute_query` | Execute SQL queries | `query`, `limit` |
| `get_table_info` | Get table metadata | `table_name`, `catalog`, `schema` |
| `compare_tables` | Full data comparison | `table1`, `table2`, `diff_lines` |
| `quick_compare_tables` | Metadata comparison | `table1`, `table2` |

</div>

---

### execute_query

Execute a SQL query on Databricks.

**Parameters:**
- `query` (str): SQL query to execute
- `limit` (int, optional): Maximum rows to return (default: 1000)

### get_table_info

Get information about a Databricks table.

**Parameters:**
- `table_name` (str): Name of the table
- `catalog` (str, optional): Catalog name
- `schema` (str, optional): Schema name

### compare_tables

Compare data between two tables by downloading full data and running diff.

**Parameters:**
- `table1` (str): First table name
- `table2` (str): Second table name
- `catalog1` (str, optional): Catalog for table1
- `schema1` (str, optional): Schema for table1
- `catalog2` (str, optional): Catalog for table2
- `schema2` (str, optional): Schema for table2
- `diff_lines` (int, optional): Number of diff context lines (default: 10)

### quick_compare_tables

Quick metadata-only comparison without downloading data.

**Parameters:**
- `table1` (str): First table name
- `table2` (str): Second table name
- `catalog1` (str, optional): Catalog for table1
- `schema1` (str, optional): Schema for table1
- `catalog2` (str, optional): Catalog for table2
- `schema2` (str, optional): Schema for table2

## VS Code MCP Integration

Add this configuration to your VS Code settings (`mcp.json`):

<details>
<summary><strong>Click to expand VS Code configuration</strong></summary>

```json
{
    "inputs": [
        {
            "type": "promptString",
            "id": "databricks_server_hostname",
            "description": "Databricks Server Hostname"
        },
        {
            "type": "promptString",
            "id": "databricks_http_path",
            "description": "Databricks HTTP Path"
        },
        {
            "type": "promptString",
            "id": "databricks_access_token",
            "description": "Databricks Access Token",
            "password": true
        },
        {
            "type": "promptString",
            "id": "databricks_catalog",
            "description": "Databricks Catalog (default: main)"
        },
        {
            "type": "promptString",
            "id": "databricks_schema",
            "description": "Databricks Schema (default: default)"
        }
    ],
    "servers": {
        "databricks": {
            "command": "python",
            "args": [
                "${workspaceFolder}/databricks_server.py"
            ],
            "env": {
                "PYTHONUNBUFFERED": "1",
                "DATABRICKS_SERVER_HOSTNAME": "${input:databricks_server_hostname}",
                "DATABRICKS_HTTP_PATH": "${input:databricks_http_path}",
                "DATABRICKS_ACCESS_TOKEN": "${input:databricks_access_token}",
                "DATABRICKS_CATALOG": "${input:databricks_catalog}",
                "DATABRICKS_SCHEMA": "${input:databricks_schema}"
            },
            "workingDirectory": "${workspaceFolder}"
        }
    }
}
```

</details>

---

<div align="center">

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

**Made with ❤️ for the Databricks community**

</div>
