import logging
import sys

logging.basicConfig(format='%(message)s')
stdout_handler = logging.StreamHandler(stream=sys.stdout)

logger = logging.Logger(name='stdout_logger', level=logging.DEBUG)
logger.addHandler(stdout_handler)
