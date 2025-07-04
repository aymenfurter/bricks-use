"""Configuration module for Databricks MCP Server."""

import logging
import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Constants
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "default"
DEFAULT_TEMP_DIR = "/tmp/databricks_mcp"

# Environment variable names
ENV_SERVER_HOSTNAME = "DATABRICKS_SERVER_HOSTNAME"
ENV_HTTP_PATH = "DATABRICKS_HTTP_PATH"
ENV_ACCESS_TOKEN = "DATABRICKS_ACCESS_TOKEN"
ENV_CATALOG = "DATABRICKS_CATALOG"
ENV_SCHEMA = "DATABRICKS_SCHEMA"
ENV_TEMP_DIR = "DATABRICKS_TEMP_DIR"

# Required environment variables
REQUIRED_ENV_VARS = [ENV_SERVER_HOSTNAME, ENV_HTTP_PATH, ENV_ACCESS_TOKEN]


@dataclass
class DatabricksConfig:
    """Configuration for the Databricks MCP Server."""

    server_hostname: str
    http_path: str
    access_token: str
    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA
    temp_dir: str = DEFAULT_TEMP_DIR

    @classmethod
    def load_from_env(cls) -> "DatabricksConfig":
        """Load configuration from environment variables or .env file.

        Returns:
            DatabricksConfig instance

        Raises:
            ValueError: If required configuration is missing
        """
        load_dotenv()

        # Get environment variables
        env_values = cls._get_environment_values()

        # Validate required values
        cls._validate_required_values(env_values)

        return cls(
            server_hostname=env_values[ENV_SERVER_HOSTNAME],
            http_path=env_values[ENV_HTTP_PATH],
            access_token=env_values[ENV_ACCESS_TOKEN],
            catalog=env_values[ENV_CATALOG],
            schema=env_values[ENV_SCHEMA],
            temp_dir=env_values[ENV_TEMP_DIR],
        )

    @classmethod
    def _get_environment_values(cls) -> dict:
        """Get all environment values with defaults.

        Returns:
            Dictionary of environment variable values.
        """
        return {
            ENV_SERVER_HOSTNAME: os.getenv(ENV_SERVER_HOSTNAME),
            ENV_HTTP_PATH: os.getenv(ENV_HTTP_PATH),
            ENV_ACCESS_TOKEN: os.getenv(ENV_ACCESS_TOKEN),
            ENV_CATALOG: os.getenv(ENV_CATALOG, DEFAULT_CATALOG),
            ENV_SCHEMA: os.getenv(ENV_SCHEMA, DEFAULT_SCHEMA),
            ENV_TEMP_DIR: os.getenv(ENV_TEMP_DIR, DEFAULT_TEMP_DIR),
        }

    @classmethod
    def _validate_required_values(cls, env_values: dict) -> None:
        """Validate that required environment variables are set.

        Args:
            env_values: Dictionary of environment variable values.

        Raises:
            ValueError: If required values are missing.
        """
        missing_vars = [var for var in REQUIRED_ENV_VARS if not env_values[var]]

        if missing_vars:
            cls._raise_missing_config_error(missing_vars)

    @classmethod
    def _raise_missing_config_error(cls, missing_vars: List[str]) -> None:
        """Raise an error for missing configuration variables.

        Args:
            missing_vars: List of missing variable names.

        Raises:
            ValueError: Always raised with descriptive message.
        """
        var_list = ", ".join(missing_vars)
        error_msg = f"Missing Databricks configuration: {var_list}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    def validate(self) -> None:
        """Validate the configuration values.

        Raises:
            ValueError: If configuration is invalid.
        """
        self._validate_hostname()
        self._validate_http_path()
        self._validate_temp_dir()

    def _validate_hostname(self) -> None:
        """Validate server hostname."""
        if not self.server_hostname or not self.server_hostname.strip():
            raise ValueError("Server hostname cannot be empty")

    def _validate_http_path(self) -> None:
        """Validate HTTP path."""
        if not self.http_path or not self.http_path.strip():
            raise ValueError("HTTP path cannot be empty")
        if not self.http_path.startswith("/"):
            raise ValueError("HTTP path must start with '/'")

    def _validate_temp_dir(self) -> None:
        """Validate temporary directory path."""
        if not self.temp_dir or not self.temp_dir.strip():
            raise ValueError("Temporary directory cannot be empty")
