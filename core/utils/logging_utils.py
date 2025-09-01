"""
Structured Logging Utilities for SpatiaEngine
"""
import logging
import sys
from typing import Optional
from pathlib import Path
import os

def setup_logging(log_level: str = "INFO", 
                  log_file: Optional[str] = None,
                  log_format: Optional[str] = None) -> logging.Logger:
    """
    Set up structured logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        log_format: Optional custom log format
        
    Returns:
        Configured logger instance
    """
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create root logger
    logger = logging.getLogger('spatiaengine')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        logger.addHandler(file_handler)
    
    # Set levels for specific loggers
    logging.getLogger('fiona').setLevel(logging.WARNING)
    logging.getLogger('rasterio').setLevel(logging.WARNING)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (will be prefixed with 'spatiaengine.')
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f'spatiaengine.{name}')

def setup_dual_logging(terminal_level: str = "INFO", 
                       file_level: str = "DEBUG",
                       log_dir: str = "logs") -> logging.Logger:
    """
    Set up dual logging (terminal and file) with different levels.
    
    Args:
        terminal_level: Log level for terminal output
        file_level: Log level for file output
        log_dir: Directory for log files
        
    Returns:
        Configured logger instance
    """
    # Create logs directory
    Path(log_dir).mkdir(exist_ok=True)
    
    # Log filename with timestamp
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"spatiaengine_{timestamp}.log")
    
    # Detailed format for file logging
    file_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s'
    
    # Simple format for terminal
    terminal_format = '%(levelname)s - %(message)s'
    
    # Create logger
    logger = logging.getLogger('spatiaengine')
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Terminal handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(terminal_format))
    console_handler.setLevel(getattr(logging, terminal_level.upper()))
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(file_format))
    file_handler.setLevel(getattr(logging, file_level.upper()))
    logger.addHandler(file_handler)
    
    return logger

class LogContext:
    """Context manager for adding context to log messages."""
    
    def __init__(self, logger: logging.Logger, context: str):
        self.logger = logger
        self.context = context
        self.original_handlers = []
    
    def __enter__(self):
        # Add context to formatter
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                original_formatter = handler.formatter
                self.original_handlers.append((handler, original_formatter))
                new_format = f'[%(levelname)s] [{self.context}] %(message)s'
                handler.setFormatter(logging.Formatter(new_format))
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original formatters
        for handler, original_formatter in self.original_handlers:
            handler.setFormatter(original_formatter)