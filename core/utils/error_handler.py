"""
Unified Error Handling System for SpatiaEngine
"""
import logging
import traceback
from typing import Optional, Callable, Any
from functools import wraps

class SpatiaEngineError(Exception):
    """Base exception class for SpatiaEngine."""
    pass

class ConfigurationError(SpatiaEngineError):
    """Configuration-related errors."""
    pass

class DataSourceError(SpatiaEngineError):
    """Data source-related errors."""
    pass

class ProcessingError(SpatiaEngineError):
    """Data processing-related errors."""
    pass

class AOIError(SpatiaEngineError):
    """Area of Interest-related errors."""
    pass

class ValidationError(SpatiaEngineError):
    """Configuration validation errors."""
    pass

def handle_errors(error_type: type = SpatiaEngineError, 
                  default_return: Any = None,
                  log_level: int = logging.ERROR) -> Callable:
    """
    Decorator for consistent error handling.
    
    Args:
        error_type: Specific exception type to catch
        default_return: Default return value on error
        log_level: Logging level for error messages
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except error_type as e:
                logging.log(log_level, f"Error in {func.__name__}: {str(e)}")
                if log_level == logging.DEBUG:
                    logging.debug(traceback.format_exc())
                return default_return
            except Exception as e:
                logging.error(f"Unexpected error in {func.__name__}: {str(e)}")
                logging.debug(traceback.format_exc())
                return default_return
        return wrapper
    return decorator

def safe_execute(func: Callable, *args, **kwargs) -> tuple:
    """
    Safely execute a function and return result with success status.
    
    Args:
        func: Function to execute
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Tuple of (success: bool, result: Any, error: Optional[Exception])
    """
    try:
        result = func(*args, **kwargs)
        return True, result, None
    except Exception as e:
        logging.error(f"Error executing {func.__name__}: {str(e)}")
        logging.debug(traceback.format_exc())
        return False, None, e

class ErrorContext:
    """Context manager for error handling with additional context."""
    
    def __init__(self, operation_name: str, context: str = ""):
        self.operation_name = operation_name
        self.context = context
        self.logger = logging.getLogger('spatiaengine.error')
    
    def __enter__(self):
        self.logger.debug(f"Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            error_msg = f"Error in {self.operation_name}"
            if self.context:
                error_msg += f" ({self.context})"
            error_msg += f": {str(exc_val)}"
            self.logger.error(error_msg)
            if exc_tb:
                self.logger.debug(traceback.format_exception(exc_type, exc_val, exc_tb))
            return True  # Suppress the exception
        else:
            self.logger.debug(f"Completed operation: {self.operation_name}")
            return False