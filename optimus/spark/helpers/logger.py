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
        self.is_active = False

    def level(self, log_level):
        """
        Set the logging message level
        :param log_level:
        :return:
        """
        self.logger.setLevel(log_level)

    def print(self, *args, **kwargs):
        """
        Print a message
        :param self:
        :return:
        """
        if self.is_active is True:
            self.logger.info(*args, **kwargs)

    def active(self, activate):
        """
        Turn on and off the logging message
        :param activate:
        :return:
        """
        self.is_active = activate


logger = Logger()
logger.level(logging.INFO)


def level(log_level):
    logger.level(log_level)


def info(message):
    logger.print(message)


def active(active=None):
    logger.active(active)
