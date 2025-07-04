"""Entry point for the Databricks MCP Server."""
import logging
import sys
import os
from typing import Optional
from src.server import DatabricksMCPServer
from src.config import DatabricksConfig

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure logging for the application."""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def load_config() -> Optional[DatabricksConfig]:
    """Load configuration with proper error handling.
    
    Returns:
        DatabricksConfig instance or None if loading failed
    """
    try:
        return DatabricksConfig.load_from_env()
    except ValueError as e:
        logger.critical(f"Configuration error: {e}")
        logger.info("Please ensure the following environment variables are set:")
        logger.info("- DATABRICKS_SERVER_HOSTNAME")
        logger.info("- DATABRICKS_HTTP_PATH")
        logger.info("- DATABRICKS_ACCESS_TOKEN")
        return None
    except Exception as e:
        logger.critical(f"Unexpected error loading configuration: {e}", exc_info=True)
        return None


def initialize_server(config: DatabricksConfig) -> Optional[DatabricksMCPServer]:
    """Initialize the MCP server.
    
    Args:
        config: Databricks configuration
        
    Returns:
        Initialized server or None if initialization failed
    """
    try:
        server = DatabricksMCPServer()
        if server.initialize(config):
            logger.info("Server initialized successfully")
            return server
        else:
            logger.critical("Failed to initialize server")
            return None
    except Exception as e:
        logger.critical(f"Server initialization failed: {e}", exc_info=True)
        return None


def main() -> int:
    """Run the MCP server.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    setup_logging()
    logger.info("Starting Databricks MCP Server...")
    
    # Load configuration
    config = load_config()
    if not config:
        return 1
    
    # Initialize server
    server = initialize_server(config)
    if not server:
        return 1
    
    # Run server
    try:
        logger.info("Starting MCP server...")
        server.run()
        return 0
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        return 0
    except Exception as e:
        logger.critical(f"Server runtime error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
