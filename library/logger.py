from library.variables import Variables
import logging
import datetime
import os

class Logger:
    def __init__(self, file_name: str, level:int = logging.DEBUG):
        current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.file_name = f"{file_name}_{current_ts}.log"
        self.variables = Variables("LOG")
        self.config = self.variables.get_variable()
        self.log_path = os.path.join(self.config.get("log_dir_name"), self.file_name)
        self.logger = logging.getLogger(self.file_name)
        self.logger.setLevel(level)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        file_handler = logging.FileHandler(self.log_path)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        print("Logger initialized")

    def log_info(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)
