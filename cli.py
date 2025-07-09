#!/usr/bin/env python3
"""Command-line interface for Databricks MCP Server tools."""

import argparse
import sys
import json
import logging
from typing import Optional, Dict, Any

from src.config import DatabricksConfig
from src.services.databricks_service import DatabricksService
from src.tools.query_tool import QueryTool
from src.tools.table_compare_tool import TableCompareTool


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for CLI."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s' if not verbose else '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def load_config() -> Optional[DatabricksConfig]:
    """Load Databricks configuration."""
    try:
        return DatabricksConfig.load_from_env()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        print("Please ensure required environment variables are set.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Unexpected configuration error: {e}", file=sys.stderr)
        return None


def execute_query_cmd(args: argparse.Namespace, client: DatabricksService) -> int:
    """Execute SQL query command."""
    tool = QueryTool(client)
    
    try:
        # Create a mock context for CLI usage
        class MockContext:
            async def info(self, msg): pass
            async def error(self, msg): pass
        
        import asyncio
        result = asyncio.run(tool.execute_query(MockContext(), query=args.query, limit=args.limit))
        
        if args.format == 'json':
            print(json.dumps(result, indent=2))
        else:
            # Pretty print table format
            if not result.get('success', False):
                print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
                return 1
            
            columns = result.get('columns', [])
            rows = result.get('data', [])
            
            if not rows:
                print("No results returned.")
                return 0
            
            # Calculate column widths
            widths = [len(col) for col in columns]
            for row in rows:
                for i, cell in enumerate(row):
                    widths[i] = max(widths[i], len(str(cell)))
            
            # Print header
            header = ' | '.join(col.ljust(widths[i]) for i, col in enumerate(columns))
            print(header)
            print('-' * len(header))
            
            # Print rows
            for row in rows:
                print(' | '.join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))
            
            print(f"\nReturned {len(rows)} rows.")
        
        return 0
    except Exception as e:
        print(f"Error executing query: {e}", file=sys.stderr)
        return 1


def table_info_cmd(args: argparse.Namespace, client: DatabricksService) -> int:
    """Get table information command."""
    tool = QueryTool(client)
    
    try:
        # Create a mock context for CLI usage
        class MockContext:
            async def info(self, msg): pass
            async def error(self, msg): pass
        
        import asyncio
        result = asyncio.run(tool.get_table_info(
            MockContext(),
            table_name=args.table,
            catalog=args.catalog,
            schema=args.schema
        ))
        
        if args.format == 'json':
            print(json.dumps(result, indent=2))
        else:
            if not result.get('success', False):
                print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
                return 1
            
            info = result['table_info']
            print(f"Table: {info['table_name']}")
            print(f"Row Count: {info['row_count']:,}")
            
            print("\nSchema:")
            for col in info['columns']:
                print(f"  {col['col_name']}: {col['data_type']}")
        
        return 0
    except Exception as e:
        print(f"Error getting table info: {e}", file=sys.stderr)
        return 1


def compare_tables_cmd(args: argparse.Namespace, client: DatabricksService) -> int:
    """Compare tables command."""
    tool = TableCompareTool(client)
    
    try:
        # Create a mock context for CLI usage
        class MockContext:
            async def info(self, msg): pass
            async def error(self, msg): pass
        
        import asyncio
        if args.quick:
            result = asyncio.run(tool.quick_compare_tables(
                MockContext(),
                table1=args.table1,
                table2=args.table2,
                catalog1=args.catalog1,
                schema1=args.schema1,
                catalog2=args.catalog2,
                schema2=args.schema2
            ))
        else:
            result = asyncio.run(tool.compare_tables(
                MockContext(),
                table1=args.table1,
                table2=args.table2,
                catalog1=args.catalog1,
                schema1=args.schema1,
                catalog2=args.catalog2,
                schema2=args.schema2,
                diff_lines=args.diff_lines
            ))
        
        if args.format == 'json':
            print(json.dumps(result, indent=2))
        else:
            if not result.get('success', False):
                print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
                return 1
            
            if args.quick:
                print("Quick comparison results:")
                print(f"Table 1 rows: {result['table1_info']['row_count']:,}")
                print(f"Table 2 rows: {result['table2_info']['row_count']:,}")
                print(f"Row difference: {result.get('row_count_difference', 0):,}")
                print(f"Columns match: {result.get('columns_match', False)}")
            else:
                print(result.get('diff_output', 'No comparison output'))
        
        return 0
    except Exception as e:
        print(f"Error comparing tables: {e}", file=sys.stderr)
        return 1


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Databricks CLI Tool')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--format', choices=['table', 'json'], default='table', help='Output format')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute SQL query')
    query_parser.add_argument('query', help='SQL query to execute')
    query_parser.add_argument('--limit', type=int, default=1000, help='Maximum rows to return')
    
    # Table info command
    info_parser = subparsers.add_parser('info', help='Get table information')
    info_parser.add_argument('table', help='Table name')
    info_parser.add_argument('--catalog', help='Catalog name')
    info_parser.add_argument('--schema', help='Schema name')
    
    # Compare tables command
    compare_parser = subparsers.add_parser('compare', help='Compare two tables')
    compare_parser.add_argument('table1', help='First table name')
    compare_parser.add_argument('table2', help='Second table name')
    compare_parser.add_argument('--catalog1', help='Catalog for first table')
    compare_parser.add_argument('--schema1', help='Schema for first table')
    compare_parser.add_argument('--catalog2', help='Catalog for second table')
    compare_parser.add_argument('--schema2', help='Schema for second table')
    compare_parser.add_argument('--diff-lines', type=int, default=10, help='Number of diff context lines')
    compare_parser.add_argument('--quick', action='store_true', help='Quick metadata-only comparison')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    setup_logging(args.verbose)
    
    # Load configuration
    config = load_config()
    if not config:
        return 1
    
    # Initialize client
    try:
        client = DatabricksService(config)
        # Test connection would need to be implemented in DatabricksService
    except Exception as e:
        print(f"Error initializing Databricks client: {e}", file=sys.stderr)
        return 1
    
    # Execute command
    try:
        if args.command == 'query':
            return execute_query_cmd(args, client)
        elif args.command == 'info':
            return table_info_cmd(args, client)
        elif args.command == 'compare':
            return compare_tables_cmd(args, client)
        else:
            print(f"Unknown command: {args.command}", file=sys.stderr)
            return 1
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
