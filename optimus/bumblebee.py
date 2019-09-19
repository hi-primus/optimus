import base64
import configparser
import json
import os
import uuid
import zlib

import requests
from cryptography.fernet import Fernet
from requests import HTTPError

from optimus.helpers.logger import logger
from optimus.helpers.output import print_html

PROTOCOL = "http://"
PROTOCOL_SSL = "https://"

DOMAIN_API = "api.hi-bumblebee.com"
FULL_DOMAIN_API = PROTOCOL_SSL + DOMAIN_API
END_POINT = FULL_DOMAIN_API + "/dataset"

DOMAIN_APP = "app.hi-bumblebee.com"
FULL_DOMAIN = PROTOCOL_SSL + DOMAIN_APP


class Comm:
    """
    Send encrypted message to the Bumblebee
    """

    def __init__(self, queue_name=None):

        # If queue_name was not given try lo load from file if not generate one
        if queue_name is None:
            self.queue_name = save_config_key("bumblebee.ini", "DEFAULT", "QueueName", str(uuid.uuid4()))

            # key is generated as byte convert to base64 so we can saved it in the config file
            key = Fernet.generate_key()
            self.key = save_config_key("bumblebee.ini", "DEFAULT", "Key", key.decode())

        else:
            self.queue_name = queue_name

        keys_link = "<a href ='{FULL_DOMAIN}'> here</a>. ".format(FULL_DOMAIN=FULL_DOMAIN,
                                                                  SESSION=self.queue_name, KEY=self.key)

        direct_link = "<a target='_blank' href ='{FULL_DOMAIN}/?session={SESSION}&key={KEY}&view=0'>call bumblebee</a>".format(
            FULL_DOMAIN=FULL_DOMAIN, SESSION=self.queue_name, KEY=self.key)

        print_html(
            "Your connection keys are in bumblebee.ini. If you really care about privacy get your keys and put them" + keys_link +
            "If you are testing just " + direct_link

        )

        self.token = None

        self.f = Fernet(self.key)

    @staticmethod
    def _encode(val):
        return base64.b64encode(val).decode('utf-8')

    @staticmethod
    def _decode(val):
        return base64.b64decode(val.encode())

    def _encrypt(self, message):
        # Convert to byte if necessary
        if not isinstance(message, (bytes, bytearray)):
            message = str(message).encode()
        return self.f.encrypt(message)

    def send(self, message):
        """
        Send the info to the queue
        :param message:
        :return:
        """
        logger.print(message)
        self.token = self._encrypt(self._compress(message)).decode()

        logger.print(message)
        try:
            headers = {'content-type': 'application/json'}

            data = json.dumps({"username": self.queue_name, "data": self.token})
            response = requests.post(END_POINT, data=data, headers=headers)

            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        else:
            print('Send!')

    def _decrypt(self, token):
        return self.f.decrypt(token)

    def receive(self, token):
        return self._decompress(self._decrypt(token))

    @staticmethod
    def _compress(message):
        """
        Compress info using zlib
        :param message:
        :return:
        """
        message = val_to_byte(message)
        return base64.b64encode(zlib.compress(message))

    @staticmethod
    def _decompress(content):
        try:
            content = zlib.decompress(base64.b64decode(content))
        except Exception:
            raise RuntimeError("Could not decode/unzip the contents")

        try:
            content = json.loads(content)
        except Exception:
            raise RuntimeError("Could interpret the unzipped contents")

        return content


def save_config_key(file_name, section="DEFAULT", key=None, value=None):
    """
    Save a key value to a section in a config file
    :param file_name: Name of hte config file
    :param section: Section
    :param key: Key
    :param value: Value
    :return:
    """

    config = configparser.ConfigParser()

    exists = os.path.isfile(file_name)
    if exists:
        # Try to load the config file
        config.read(file_name)

        if config.has_option(section, key):
            result = config[section][key]
        else:

            config.set(section, key, value)
            with open(file_name, 'w') as configfile:
                config.write(configfile)
            result = value

    else:

        config.set(section, key, value)
        with open(file_name, 'w') as configfile:
            config.write(configfile)
        result = value
    return result


def val_to_byte(value):
    if not isinstance(value, (bytes, bytearray)):
        value = str(value).encode()
    return value
