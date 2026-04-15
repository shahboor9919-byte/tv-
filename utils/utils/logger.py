import logging
import colorlog
import sys

def setup_logger(name: str = "iptv_aggregator", level: int = logging.INFO) -> logging.Logger:
    """Configure structured colored logging."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()
