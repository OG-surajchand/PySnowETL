import os
import logging
import datetime
from dotenv import load_dotenv
from config.constants import *

load_dotenv()

class Logger:
    def __init__(self, log_name, file_mode='w'):
        log_path = os.getenv('log_path')
        if not log_path:
            raise ValueError("Environment variable 'log_path' not set.")
        
        current_ts = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
        log_file_name = f'{log_name}_{current_ts}.log'
        self.log_path = os.path.join(log_path, log_file_name)
        self.logger = self.get_logger(file_mode)

    def get_logger(self, file_mode):
        logging.basicConfig(filename=self.log_path, format='%(asctime)s %(message)s', filemode=file_mode)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        return logger

    def info(self, message):
        self.logger.info(f"[INFO]: {message}")

    def error(self, message):
        self.logger.error(f"[ERROR]: {message}")
