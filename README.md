# Databricks MCP Server (Unofficial)

A Model Context Protocol (MCP) server for executing Databricks SQL queries and comparing table data.

This project is purely meant for demo purpose, use at your own risk!

## Features

- **Execute SQL Queries**: Run any SQL query on Databricks with configurable result limits
- **Table Information**: Get detailed information about tables including schema and row counts
- **Table Comparison**: Compare two tables by downloading their data and running CLI diff
- **Quick Comparison**: Fast metadata-only comparison of tables

## Setup

### Prerequisites
- Python 3.11+
- Databricks workspace access
- Databricks personal access token

### Environment Variables

Set the following environment variables or create a `.env` file:

```bash
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-personal-access-token
DATABRICKS_CATALOG=main  # Optional, defaults to 'main'
DATABRICKS_SCHEMA=default  # Optional, defaults to 'default'
DATABRICKS_TEMP_DIR=/tmp/databricks_mcp  # Optional temp directory
```

### Installation

1. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the server:
```bash
python databricks_server.py
```

## Tools

### execute_query
Execute a SQL query on Databricks.

**Parameters:**
- `query` (str): SQL query to execute
- `limit` (int, optional): Maximum rows to return (default: 1000)

**Example:**
```
Execute this query: SELECT * FROM my_table WHERE status = 'active'
```

### get_table_info
Get information about a Databricks table.

**Parameters:**
- `table_name` (str): Name of the table
- `catalog` (str, optional): Catalog name
- `schema` (str, optional): Schema name

**Example:**
```
Get info for table 'users' in catalog 'production' and schema 'analytics'
```

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

**Example:**
```
Compare tables 'users_old' and 'users_new' and show differences
```

### quick_compare_tables
Quick metadata-only comparison without downloading data.

**Parameters:**
- `table1` (str): First table name
- `table2` (str): Second table name
- `catalog1` (str, optional): Catalog for table1
- `schema1` (str, optional): Schema for table1
- `catalog2` (str, optional): Catalog for table2
- `schema2` (str, optional): Schema for table2

**Example:**
```
Quick compare 'table_a' and 'table_b' schemas and row counts
```

## VS Code MCP Integration

Add this configuration to your VS Code settings (mcp.json):

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

## Usage Examples

### Execute a Query
```
Execute this SQL query: SELECT customer_id, order_date, total_amount FROM orders WHERE order_date >= '2024-01-01' LIMIT 100
```

### Compare Tables
```
Compare tables 'sales_2023' and 'sales_2024' and show me the differences
```

### Quick Schema Comparison
```
Quick compare the schemas of 'old_users' and 'new_users' tables
```

## License

This project is licensed under the MIT License.
