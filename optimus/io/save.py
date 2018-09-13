import logging

from kombu import Connection, Exchange, Queue, Producer
from pymongo import MongoClient
from pyspark.sql import DataFrame

from optimus.helpers.decorators import *


def save(self):
    @add_attr(save)
    def json(path, mode="overwrite", num_partitions=1):
        """
        Save data frame in a json file
        :param path: path where the dataframe will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                "append": Append contents of this DataFrame to existing data.
                "overwrite" (default case): Overwrite existing data.
                "ignore": Silently ignore this operation if data already exists.
                "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """
        try:
            # na.fill enforce null value keys to the json output
            self.na.fill("") \
                .repartition(num_partitions) \
                .write \
                .format("json") \
                .mode(mode) \
                .save(path)
        except IOError as e:
            logging.error(e)
            raise

    @add_attr(save)
    def csv(path, header="true", mode="overwrite", sep=",", num_partitions=1):
        """
        Save data frame to a CSV file.
        :param path: path where the dataframe will be saved.
        :param header: True or False to include header
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param sep: sets the single character as a separator for each field and value. If None is set,
        it uses the default value.
        :param num_partitions: the number of partitions of the DataFrame
        :return: Dataframe in a CSV format in the specified path.
        """

        try:
            self.repartition(num_partitions).write.options(header=header).mode(mode).csv(path, sep=sep)
        except IOError as error:
            logging.error(error)
            raise

    @add_attr(save)
    def parquet(path, mode="overwrite", num_partitions=1):
        """
        Save data frame to a parquet file
        :param path: path where the dataframe will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """
        # This character are invalid as column names by parquet
        invalid_character = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = self.cols.rename(func)

        try:
            df.coalesce(num_partitions) \
                .write \
                .mode(mode) \
                .parquet(path)
        except IOError as e:
            logging.error(e)
            raise

    @add_attr(save)
    def avro(path, mode="overwrite", num_partitions=1):
        """
        Save data frame to an avro file
        :param path: path where the dataframe will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """

        try:
            self.coalesce(num_partitions) \
                .write.format("com.databricks.spark.avro") \
                .mode(mode) \
                .save(path)
        except IOError as e:
            logging.error(e)
            raise

    @add_attr(save)
    def rabbit_mq(host, exchange_name=None, queue_name=None, routing_key=None, parallelism=None):
        """
        Send a dataframe to a redis queue
        # https://medium.com/python-pandemonium/talking-to-rabbitmq-with-python-and-kombu-6cbee93b1298
        # https://medium.com/python-pandemonium/building-robust-rabbitmq-consumers-with-python-and-kombu-part-1-ccd660d17271
        :return:
        """
        df = self
        if parallelism:
            df = df.coalesce(parallelism)

        def _rabbit_mq(messages):
            conn = Connection(host)
            channel = conn.channel()

            exchange = Exchange(exchange_name, type="direct")
            queue = Queue(name=queue_name, exchange=exchange, routing_key=routing_key)

            queue.maybe_bind(conn)
            queue.declare()
            producer = Producer(exchange=exchange, channel=channel, routing_key=routing_key)

            for message in messages:
                # as_dict = message.asDict(recursive=True)
                producer.publish(message)

            channel.close()
            conn.release()
            return messages

        self.rdd.mapPartitions(_rabbit_mq).count()

    @add_attr(save)
    def mongo(host, port=None, db_name=None, collection_name=None, parallelism=None):
        """
        Send a dataframe to a mongo collection
        :param host:
        :param port:
        :param db_name:
        :param collection_name:
        :param parallelism:
        :return:
        """
        df = self
        if parallelism:
            df = df.coalesce(parallelism)

        def _mongo(messages):
            client = MongoClient(host, port)
            db = client[db_name]
            collection = db[collection_name]

            for message in messages:

                as_dict = message.asDict(recursive=True)
                collection.insert_one(as_dict)
            client.close()
            return messages

        df.rdd.mapPartitions(_mongo).count()

    return save


DataFrame.save = property(save)
