import logging
import logging.config
from typing import Optional
from src.config import LOG_CONFIG

def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Set up and return a logger instance.
    
    Args:
        name: Optional name for the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    try:
        logging.config.dictConfig(LOG_CONFIG)
        logger = logging.getLogger(name or __name__)
        return logger
    except Exception as e:
        # Fallback to basic configuration if the custom config fails
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(name or __name__)
        logger.error(f"Failed to configure logger with custom config: {str(e)}")
        return logger 