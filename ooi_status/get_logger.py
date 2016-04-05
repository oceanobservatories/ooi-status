import logging


root_logger = None


def setup(level=logging.WARN):
    logger = logging.getLogger()
    logger.setLevel(level)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


def get_logger(name, level=logging.WARN):
    global root_logger
    if root_logger is None:
        root_logger = setup(level)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    return logger
