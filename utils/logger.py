# import logging
# import os
# from crawler.crawlerconfig import CRAWLER_CONFIG
#
#
# def get_file_logger(log_filename: str):
#     """
#     Creates a logger for a specific well log file (LAS/DLIS) inside the defined log folder.
#     """
#     log_dir = CRAWLER_CONFIG["LOG_FOLDER"]  # Use the configured log folder
#     os.makedirs(log_dir, exist_ok=True)  # Ensure log directory exists
#
#     log_filepath = os.path.join(log_dir, log_filename)
#
#     logger = logging.getLogger(log_filename)
#     logger.setLevel(logging.DEBUG)  # Capture all logs
#
#     # File handler
#     file_handler = logging.FileHandler(log_filepath, mode='a')
#     file_handler.setLevel(logging.DEBUG)
#
#     # Console handler
#     console_handler = logging.StreamHandler()
#     console_handler.setLevel(logging.INFO)
#
#     # Formatter
#     formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     file_handler.setFormatter(formatter)
#     console_handler.setFormatter(formatter)
#
#     if not logger.hasHandlers():
#         logger.addHandler(file_handler)
#         logger.addHandler(console_handler)
#
#     return logger

import logging
import os

class Logger:
    _loggers = {}  # Dictionary to store loggers per file
    _log_dir = None  # Store log directory only once

    @staticmethod
    def initialize_log_dir():
        """ Ensures `_log_dir` is set before using it, creating 'logs' in the project root. """
        if Logger._log_dir is None:
            project_root = os.path.dirname(os.path.abspath(__file__))  # Get the current script's directory
            Logger._log_dir = os.path.join(project_root, "..", "logs")  # Set logs folder in root
            Logger._log_dir = os.path.abspath(Logger._log_dir)  # Ensure absolute path
            os.makedirs(Logger._log_dir, exist_ok=True)  # Ensure log directory exists

    @staticmethod
    def get_logger(log_filename: str):
        """
        Returns a logger for a specific well log file (LAS/DLIS) inside the defined log folder.
        If the logger already exists, returns the existing one.
        """
        Logger.initialize_log_dir()  # ✅ Ensure log directory is set before proceeding

        # Verify `_log_dir` is set
        if Logger._log_dir is None:
            raise ValueError("Logger._log_dir is not initialized.")

        log_filepath = os.path.join(Logger._log_dir, log_filename)

        if log_filename not in Logger._loggers:
            logger = logging.getLogger(log_filename)
            logger.setLevel(logging.DEBUG)  # Capture all logs

            # File handler
            file_handler = logging.FileHandler(log_filepath, mode='a')
            file_handler.setLevel(logging.DEBUG)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Formatter
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers only if they are not already present
            if not logger.hasHandlers():
                logger.addHandler(file_handler)
                logger.addHandler(console_handler)

            Logger._loggers[log_filename] = logger  # Store logger in dictionary

        return Logger._loggers[log_filename]  # Return existing or newly created logger