import logging
import sys

def setup_logging(level=logging.INFO):
    logger = logging.getLogger()
    if logger.hasHandlers():
        return  # already configured

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s:\n%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)