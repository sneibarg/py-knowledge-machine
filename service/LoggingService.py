import logging
import os
import sys
from datetime import datetime


class LoggingService:
    def __init__(self, logger_dir, logger_name):
        self.logger_dir = logger_dir
        self.logger_name = logger_name
        self.pid = os.getpid()
        pass

    @staticmethod
    def set_logger_level(logger, level):
        logger.setLevel(level)

    @staticmethod
    def set_logger_name(logger_name):
        logger = logging.getLogger(logger_name)
        logger.name = logger_name

    @staticmethod
    def set_logger_formatter(logger_name, formatter):
        logger = logging.getLogger(logger_name)
        logger.setFormatter(formatter)

    def setup_logging(self, debug=False):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.logger_dir, f"application_{timestamp}_{self.pid}.log")
        logging.getLogger('').handlers = []
        new_logger = logging.getLogger(self.logger_name)
        new_logger.setLevel(logging.INFO if not debug else logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s [PID %(process)d] [%(levelname)s] [%(name)s] %(message)s")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        new_logger.addHandler(file_handler)

        if debug:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            if sys.platform.startswith('win'):
                console_handler.stream = sys.stdout
                console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
            new_logger.addHandler(console_handler)

        return new_logger
