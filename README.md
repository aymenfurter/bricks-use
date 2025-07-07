<div align="center">

<img src="https://raw.githubusercontent.com/aymenfurter/bricks-use/refs/heads/main/bricks-use.png" alt="Project Logo" width="550" />

[![CI/CD Pipeline](https://github.com/aymenfurter/bricks-use/actions/workflows/ci.yml/badge.svg)](https://github.com/aymenfurter/bricks-use/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Type Checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](https://mypy.readthedocs.io/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-red.svg)](https://databricks.com/)

**A powerful Model Context Protocol (MCP) server for executing Databricks SQL queries and comparing table data.**

*⚠️ This project is purely meant for demo purposes - use at your own risk!*

</div>

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Setup](#setup)
- [Tools](#tools)
- [VS Code Integration](#vs-code-integration)
- [Usage Examples](#usage-examples)
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

# 4. Run the server
python databricks_server.py
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

3. **Run the server:**
   ```bash
   python databricks_server.py
   ```

## Tools

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

> **Parameters:**
> - `query` (str): SQL query to execute
> - `limit` (int, optional): Maximum rows to return (default: 1000)

**Example:**
```sql
Execute this query: SELECT * FROM my_table WHERE status = 'active'
```

### get_table_info

Get information about a Databricks table.

> **Parameters:**
> - `table_name` (str): Name of the table
> - `catalog` (str, optional): Catalog name
> - `schema` (str, optional): Schema name

**Example:**
```
Get info for table 'users' in catalog 'production' and schema 'analytics'
```

### compare_tables

Compare data between two tables by downloading full data and running diff.

> **Parameters:**
> - `table1` (str): First table name
> - `table2` (str): Second table name
> - `catalog1` (str, optional): Catalog for table1
> - `schema1` (str, optional): Schema for table1
> - `catalog2` (str, optional): Catalog for table2
> - `schema2` (str, optional): Schema for table2
> - `diff_lines` (int, optional): Number of diff context lines (default: 10)

**Example:**
```
Compare tables 'users_old' and 'users_new' and show differences
```

### quick_compare_tables

Quick metadata-only comparison without downloading data.

> **Parameters:**
> - `table1` (str): First table name
> - `table2` (str): Second table name
> - `catalog1` (str, optional): Catalog for table1
> - `schema1` (str, optional): Schema for table1
> - `catalog2` (str, optional): Catalog for table2
> - `schema2` (str, optional): Schema for table2

**Example:**
```
Quick compare 'table_a' and 'table_b' schemas and row counts
```

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

## Usage Examples

### Execute a Query
```sql
Execute this SQL query: SELECT customer_id, order_date, total_amount 
                       FROM orders 
                       WHERE order_date >= '2024-01-01' 
                       LIMIT 100
```

### Compare Tables
```
Compare tables 'sales_2023' and 'sales_2024' and show me the differences
```

### Quick Schema Comparison
```
Quick compare the schemas of 'old_users' and 'new_users' tables
```

---

<div align="center">

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

**Made with ❤️ for the Databricks community**

</div>
