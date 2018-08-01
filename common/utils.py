import os
import logging
from logging.handlers import RotatingFileHandler

logging.getLogger('asyncio').setLevel(logging.DEBUG)

FORMAT = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
DATEFMT = '%a, %d %b %Y %H:%M:%S'


def setup_log(level, file_path, max_bytes=20 * 1024 * 1024, backup_count=5):
    if not os.path.exists(os.path.split(file_path)[0]):
        os.makedirs(os.path.split(file_path)[0])
    logging.basicConfig(level=level,
                        format=FORMAT,
                        datefmt=DATEFMT)
    rotate_handler = RotatingFileHandler(file_path, maxBytes=max_bytes, backupCount=backup_count)
    rotate_handler.setLevel(level)
    rotate_handler.setFormatter(logging.Formatter(FORMAT, DATEFMT))
    logging.getLogger('').addHandler(rotate_handler)
