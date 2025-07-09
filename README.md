# Databricks MCP Server (Unofficial)

A Model Context Protocol (MCP) server for executing Databricks SQL queries and comparing table data.

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

## CLI Usage

The project includes a command-line interface that provides direct access to all Databricks tools without running the MCP server.

### Setup CLI

Make the CLI script executable:
```bash
make setup-cli
# or manually:
# chmod +x cli.py
# chmod +x bricks
```

### Easy CLI Usage (Recommended)

Use the `bricks` wrapper script for simpler commands:

```bash
# Execute SQL Query
./bricks query "SELECT * FROM my_table LIMIT 10"
./bricks query "SELECT count(*) FROM users WHERE active = true" --limit 100

# Get Table Information
./bricks info my_table
./bricks info users --catalog production --schema analytics

# Compare Tables
./bricks compare customer customer_csv
./bricks compare old_users new_users --catalog1 dev --catalog2 prod
./bricks compare table1 table2 --quick

# Output formats and options
./bricks query "SELECT * FROM users LIMIT 5" --format json
./bricks info my_table --format json
./bricks -v query "SELECT * FROM my_table"  # verbose mode
```

### CLI Examples with Output

#### Table Comparison Example
```bash
./bricks compare customer customer_csv
```

Output:
```diff
--- /tmp/databricks_mcp/sample_5_1_customer.csv 2025-07-09 10:54:43.274622013 +0000
+++ /tmp/databricks_mcp/sample_5_2_customer_csv.csv     2025-07-09 10:54:43.279622013 +0000
@@ -1,5 +1,5 @@
 customer_id,first_name,last_name,email,phone,birth_date,registration_date,is_active,credit_limit,loyalty_points
-1,Hans,Müller,hans.mueller@email.ch,+41 79 123 45 67,1985-03-15,2023-01-15 10:30:00+00:00,True,5000.00,1250
-2,Maria,Garcia,maria.garcia@email.ch,+41 78 987 65 43,1990-07-22,2023-02-20 14:15:30+00:00,True,7500.00,2100
-3,Peter,Schmidt,peter.schmidt@email.ch,+41 76 555 44 33,1978-12-08,2023-03-10 09:45:15+00:00,False,3200.00,850
-4,Anna,Weber,anna.weber@email.ch,+41 77 321 98 76,1992-05-14,2023-04-05 16:20:45+00:00,True,12000.00,3450
+1,Hans,Müller,hans.mueller@email.ch,+41 79 123 45 67,15.03.1985,2023-01-15 10:30:00,true,5'000.00,1250
+2,Maria,Garcia,maria.garcia@email.ch,+41 78 987 65 43,22.07.1990,2023-02-20 14:15:30,true,7'500.00,2100

... (truncated 3 more lines, showing first 10 lines)
```

#### Query Execution Example
```bash
./bricks query "SELECT customer_id, first_name, last_name FROM customer LIMIT 3"
```

Output:
```
customer_id | first_name | last_name
------------|------------|----------
1           | Hans       | Müller   
2           | Maria      | Garcia   
3           | Peter      | Schmidt  

Returned 3 rows.
```

#### Table Information Example
```bash
./bricks info customer --format json
```

Output:
```json
{
  "success": true,
  "table_info": {
    "table_name": "customer",
    "row_count": 50000,
    "columns": [
      {"col_name": "customer_id", "data_type": "bigint"},
      {"col_name": "first_name", "data_type": "string"},
      {"col_name": "last_name", "data_type": "string"},
      {"col_name": "email", "data_type": "string"}
    ]
  }
}
```

### Traditional CLI Commands

You can also use the Python script directly:

#### Execute SQL Query
```bash
python cli.py query "SELECT * FROM my_table LIMIT 10"
python cli.py query "SELECT count(*) FROM users WHERE active = true" --limit 100
```

#### Get Table Information
```bash
python cli.py info my_table
python cli.py info users --catalog production --schema analytics
```

#### Compare Tables
```bash
# Full data comparison
python cli.py compare table1 table2
python cli.py compare old_users new_users --catalog1 dev --catalog2 prod

# Quick metadata-only comparison
python cli.py compare table1 table2 --quick
```

#### Output Formats
```bash
# Default table format
python cli.py query "SELECT * FROM users LIMIT 5"

# JSON format
python cli.py query "SELECT * FROM users LIMIT 5" --format json
python cli.py info my_table --format json
```

#### Verbose Mode
```bash
python cli.py -v query "SELECT * FROM my_table"
```

### CLI Examples

```bash
# Execute a complex query
./bricks query "
SELECT 
    customer_id, 
    SUM(order_total) as total_spent,
    COUNT(*) as order_count
FROM orders 
WHERE order_date >= '2024-01-01' 
GROUP BY customer_id 
ORDER BY total_spent DESC 
LIMIT 20
"

# Get detailed table information
./bricks info sales_data --catalog main --schema analytics

# Compare two versions of a table
./bricks compare sales_2023 sales_2024 --diff-lines 5

# Quick schema comparison
./bricks compare old_schema new_schema --quick --format json
```

## License

This project is licensed under the MIT License.
