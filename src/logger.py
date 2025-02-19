import logging
from datetime import datetime
import os

# Global logger instance
_logger = None


def setup_logger(name: str) -> logging.Logger:
    global _logger

    # If logger already exists, return a child logger with the given name
    if _logger is not None:
        return _logger.getChild(name)

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Create the root logger only once
    _logger = logging.getLogger("pipeline")
    _logger.setLevel(logging.INFO)

    # Only add handlers if they haven't been added yet
    if not _logger.handlers:
        # Create formatters and handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        _logger.addHandler(console_handler)

        # File handler - use a single file with timestamp
        log_file = f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)

    # Return a child logger with the given name
    return _logger.getChild(name)
