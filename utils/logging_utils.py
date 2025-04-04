"""
Logging utilities for the South Korea COVID-19 vaccination analysis project.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
import os

from config import LOG_LEVEL, LOG_FORMAT, LOG_FILE, LOGS_DIR

def setup_logger(name, log_file=LOG_FILE, level=LOG_LEVEL):
    """
    Sets up a logger with console and file handlers.
    
    Args:
        name (str): Name of the logger
        log_file (str): Path to the log file
        level (str): Logging level
        
    Returns:
        logging.Logger: Configured logger
    """
    # Create logs directory if it doesn't exist
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    # Convert string level to logging level
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatters
    formatter = logging.Formatter(LOG_FORMAT)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Create file handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10485760, backupCount=5
    )  # 10MB max size, 5 backup files
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def log_function_call(logger):
    """
    Decorator to log function calls.
    
    Args:
        logger (logging.Logger): Logger to use
        
    Returns:
        function: Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"Calling {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            logger.debug(f"{func.__name__} completed")
            return result
        return wrapper
    return decorator

def log_error(logger):
    """
    Decorator to log errors.
    
    Args:
        logger (logging.Logger): Logger to use
        
    Returns:
        function: Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
                raise
        return wrapper
    return decorator