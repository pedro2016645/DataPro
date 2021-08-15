"""
Script to init a logging system
"""
import logging
import os

DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"


def create_logger(log_filename: str, log_folder: str = r".\logs", ) -> logging.getLoggerClass():
    """
    Create file for a logging system
    :param log_folder: folder where save the logs files
    :param log_filename:
    :return:
    """

    assert type(log_filename) == str, "The log_filename must be str"
    assert type(log_folder) == str, "The log_folder must be str"
    assert os.path.exists(log_folder), "The log_folder doesn't exist"

    # Add timestamp to the logger name
    log_path = os.path.join(log_folder, log_filename)

    # Create a custom logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -> %(name)s : %(levelname)s | %(message)s')
    logger = logging.getLogger()

    # Create handlers
    f_handler = logging.FileHandler(os.path.join(os.getcwd(), log_path))
    f_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    f_format = logging.Formatter('%(asctime)s -> %(name)s : %(levelname)s | %(message)s')
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(f_handler)

    # Inform the user where the file is located
    logger.info("The file " + log_path + " was created ")
    return logger
