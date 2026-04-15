import logging

def setup_logger(name="iptv_engine", level=logging.INFO):
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# 👇 ده السطر المهم
logger = setup_logger()
