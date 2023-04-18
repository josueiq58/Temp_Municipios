from datetime import datetime
import logging
import sys
import os

root = os.getcwd()

class Logger:
    '''
    Class to manage all actions related to log files
    '''
    def __init__(self, name: str):
        
        self.sep = '\n' + "-" * 100
        today = datetime.today().strftime('%Y-%m-%d %H_%M')
        self.log_file = f'{today} process_execution.log'
        
        # Create logs directories if they don't exist yet
        self.check_logs_directories()
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self.logger_errors = logging.getLogger(f'-{name}-')
        self.logger_errors.setLevel(logging.ERROR)

        # Formatter specifying the syntax of the logs
        formatter = logging.Formatter(fmt = '%(name)s – %(levelname)s – %(asctime)s – %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        # Create Handlers for distinct types of logs
        self.results_file = f'{root}\\logs\\results\\{self.log_file}'
        results_handler = logging.FileHandler(self.results_file, encoding = 'utf-8')
        
        self.errors_file = f'{root}\\logs\\errors\\{self.log_file}'
        errors_handler = logging.FileHandler(self.errors_file, encoding = 'utf-8')

        errors_handler.setFormatter(formatter)
        results_handler.setFormatter(formatter)

        # Add handlers to logger
        self.logger_errors.addHandler(errors_handler)
        self.logger.addHandler(results_handler)

    def check_logs_directories(self) -> None:
        '''
        Check whether or not logs directories are already created.
        '''
        logs_path = f'{root}\\logs'
        if not os.path.isdir(logs_path):
            os.mkdir(logs_path)

        directories = ['\\errors', '\\results']
        for directory in directories:
            if not os.path.isdir(logs_path + directory):
                os.mkdir(logs_path + directory)

    def log_info(self, message: str) -> None:
        self.logger.info(message)

    def log_error(self, message: str) -> None:
        self.logger_errors.error(message)

    def log_exception(self, message: str) -> None:
        self.logger_errors.exception(message)