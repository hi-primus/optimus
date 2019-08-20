import base64
import configparser
import json
import os
import time
import uuid
import zlib

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet

from optimus.helpers.logger import logger


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


IP_QUEUE = "165.22.149.93"


class Comm:
    """

    """

    def __init__(self, queue_name=None, username="mqtt-test", password="mqtt-test"):

        # If queue_name was not given try lo load from file if not generate one
        if queue_name is None:
            self.queue_name = save_config_key("bumblebee.ini", "DEFAULT", "QueueName", str(uuid.uuid4()))

            # key is generated as byte convert to base64 so we can saved it in the config file
            key = Fernet.generate_key()
            self.key = save_config_key("bumblebee.ini", "DEFAULT", "Key", key.decode())

        else:
            self.queue_name = queue_name

        # Queue config
        client = mqtt.Client("MQTT")
        client.username_pw_set(username=username, password=password)

        # Callbacks
        client.connected_flag = False

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.connected_flag = True  # set flag
                # print("connected...")
            else:
                print("Bad connection Returned code=", rc)

        def on_disconnect(client, userdata, rc):
            # print("disconnected")
            client.connected_flag = False

        def on_publish(client, userdata, result):  # create function for callback
            print("Data sent \n")

        client.on_publish = on_publish
        client.on_disconnect = on_disconnect
        client.on_connect = on_connect

        self.queue = client
        self.token = None

        self.f = Fernet(self.key)

        # print(self.key)
        # print(self.queue_name)
        # return

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
        print(message)
        self.token = self._encrypt(self._compress(message)).decode()
        self.to_queue(self.token)

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

    def to_queue(self, message):
        """
        Send the profiler information to a queue. By default it use a public encrypted queue.
        :return:


        """
        logger.print(message)
        client = self.queue
        client.connected_flag = False
        client.disconnect()

        try:
            client.connect(IP_QUEUE)
        except Exception:
            print("Connection failed. Please try again")
            exit(1)  # Should quit or raise flag to quit or retry

        client.loop_start()  # start the loop

        # print(message)
        while not client.connected_flag:  # wait in loop
            time.sleep(1)

        client.publish(self.queue_name, payload=message)

        client.loop_stop()
