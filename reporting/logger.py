"""
Centralized Logging Utility

This module provides a singleton-style logger configuration for all reporting modules.
Ensures consistent log formatting and file/console handlers across the codebase.
"""

import os
import logging
from pathlib import Path
from reporting.config import LOGS_DIR, LOG_FILENAME

# Global logger instance cache
_loggers = {}


def _setup_logger(name: str) -> logging.Logger:
    """
    Internal function to set up a logger with file and console handlers.
    
    Args:
        name: Logger name (typically __name__ from calling module)
    
    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers (singleton-style)
    if logger.handlers:
        return logger
    
    # Set logger level
    logger.setLevel(logging.DEBUG)
    
    # Create logs directory if it doesn't exist
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)
    
    # Log file path
    log_file_path = logs_path / LOG_FILENAME
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler (all levels)
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger instance for the given name.
    
    This function ensures singleton-style behavior - each module name
    gets exactly one logger instance with consistent configuration.
    
    Args:
        name: Logger name (typically __name__ from calling module)
    
    Returns:
        Configured Logger instance
    
    Example:
        from reporting.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Module initialized")
    """
    if name not in _loggers:
        _loggers[name] = _setup_logger(name)
    
    return _loggers[name]

