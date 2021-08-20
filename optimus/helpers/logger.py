import logging


class Singleton(object):
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instances[cls]


class Logger(Singleton):
    def __init__(self):
        self.logger = logging.getLogger('optimus')
        self.log_level = 20 # INFO

    def level(self, log_level):
        """
        Set the logging message level
        :param log_level:
        :return:
        """
        if log_level>=20:
            self.log_level = log_level
        self.logger.setLevel(log_level)

    def print(self, *args, **kwargs):
        """
        Print a message
        :return:
        """
        self.logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        """
        Print a warning
        :return:
        """
        self.logger.warn(*args, **kwargs)

    def active(self, activate):
        """
        Turn on and off the logging message
        :param activate:
        :return:
        """
        if activate:
            self.logger.setLevel(10)
        else:
            self.logger.setLevel(self.log_level)


logger = Logger()
logger.level(logging.INFO)
