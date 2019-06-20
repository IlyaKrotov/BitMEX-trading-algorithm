import logging
import os

def create_logger(path_in_logs=""):
    # create logger
    logger = logging.getLogger(path_in_logs)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

    if not os.path.exists('../logs/' + path_in_logs):
        os.makedirs('../logs/' + path_in_logs)

    # create console handler and set level to debug
    fh = logging.FileHandler('../logs/' + path_in_logs + '/log')
    fh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    fh.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(fh)

    return logger
