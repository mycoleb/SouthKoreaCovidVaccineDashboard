"""
Logging utilities for the South Korea COVID-19 vaccination analysis project.
"""

import os
import logging
import functools
from datetime import datetime

# Ensure logs directory exists
logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(logs_dir, exist_ok=True)

def setup_logger(name):
    """
    Set up and return a logger with file and console handlers.
    
    Args:
        name (str): Name of the logger
        
    Returns:
        logging.Logger: Configured logger
    """
    # Create logger with 'name'
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Check if logger already has handlers to avoid duplicates
    if logger.handlers:
        return logger
    
    # Create file handler which logs even debug messages
    log_file = os.path.join(logs_dir, f'{name}_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    # Create console handler with a higher log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def log_function_call(logger):
    """
    Decorator to log function calls with parameters and execution time.
    
    Args:
        logger (logging.Logger): Logger to use
        
    Returns:
        callable: Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            logger.debug(f"Entering {func_name}")
            
            # Log args and kwargs
            if args:
                logger.debug(f"{func_name} args: {args}")
            if kwargs:
                logger.debug(f"{func_name} kwargs: {kwargs}")
            
            # Execute function and measure time
            start_time = datetime.now()
            try:
                result = func(*args, **kwargs)
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                logger.debug(f"Exiting {func_name} (execution time: {execution_time:.2f}s)")
                return result
            except Exception as e:
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                logger.error(f"Exception in {func_name} after {execution_time:.2f}s: {e}", exc_info=True)
                raise
        
        return wrapper
    
    return decorator

def log_error(logger):
    """
    Decorator to log exceptions.
    
    Args:
        logger (logging.Logger): Logger to use
        
    Returns:
        callable: Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Exception in {func.__name__}: {e}", exc_info=True)
                raise
        
        return wrapper
    
    return decorator