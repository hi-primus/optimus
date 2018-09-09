import csv
import json
import logging

import pandas as pd
import requests
from pymongo import MongoClient
from pyspark.sql.functions import DataFrame
from tqdm import tqdm_notebook

from optimus.helpers.checkit import is_function, is_


class Enricher:
    """
    Enrich data from a Pandas or Spark dataframe
    """

    def __init__(self, host, port, db_name=None, collection_name=None):
        logging.basicConfig(format="%(message)s", level=logging.INFO)

        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = MongoClient(host, port)

    # FIFTEEN_MINUTES = 900
    # @limits(calls=15, period=FIFTEEN_MINUTES)

    def send(self, df):
        """
        Send the dataframe to the mongo collection
        :param df: dataframe to be send to the enricher
        :return:
        """

        if is_(df, pd.DataFrame):
            self.get_collection(self.collection_name).insert_many(df.to_dict("records"))
        elif is_(df, DataFrame):
            df.save.mongo(self.host, self.port, self.db_name, self.collection_name)
        else:
            raise Exception("df must by a Spark Dataframe or Pandas Dataframe")

    def flush(self):
        """
        Flush the enricher default collection
        :return:
        """
        count = self.count()
        self.drop_collection(self.collection_name)
        print("Removed {count} documents".format(count=count))

    def count(self):
        """
        Conunt nunber of documents in a collections
        :return:
        """
        collection = self.get_collection(self.collection_name)
        cursor = collection.find()
        return cursor.count(True)

    def run(self, collection_name=None, func_request=None, func_response=None, return_type="json",
            calls=None, period=60):
        """
        Read a the url key from a mongo collection an make a request to a service
        :param collection_name:
        :param func_request: help to create a custom request
        :param func_response: help to create a custom response
        :param return_type:
        :param calls: how many call can you make
        :param period: in which period ot time can the call be made
        :return:
        """

        if collection_name is None:
            collection_name = self.collection_name

        collection = self.get_collection(collection_name)

        cursor = collection.find({"result": {"$exists": False}})

        total_docs = cursor.count(True)
        if total_docs > 0:
            if func_request is None:
                func_request = requests.get

            for c in tqdm_notebook(cursor, total=total_docs, desc='Processing...'):

                # Send request to the API
                response = func_request(c)

                mongo_id = c["_id"]

                if response.status_code == 200:
                    if return_type == "json":
                        response = json.loads(response.text)
                    elif return_type == "text":
                        response = response.text

                    # Process the result with an external function
                    if is_function(func_response):
                        response = func_response(response)

                    # update the mongo id  with the result
                    self.get_collection(collection_name).find_and_modify(query={"_id": mongo_id},
                                                                         update={"$set": {'result': response}},
                                                                         upsert=False, full_response=True)
                else:
                    # The response key will remain blank so we can filter it to try in future request
                    print(response.status_code)
        else:
            print("No records available to process")

    def collection_exists(self, collection_name):
        """
        Check if a collection exist
        :param collection_name:
        :return:
        """
        if collection_name in self.get_db().collection_names():
            return True
        else:
            return False

    def db_exists(self, db_name):
        """
        Check if a collection exist
        :param db_name:
        :return:
        """
        if db_name in self.client.list_database_names():
            return True
        else:
            return False

    def get_db(self):
        return self.client[self.db_name]

    def get_collection(self, collection_name):
        """

        :param collection_name:
        :return:
        """
        collection = self.get_db()[collection_name]
        return collection

    def copy_collection(self, source_name, dest_name):
        """
        Copy Collection
        :param source_name:
        :param dest_name:
        :return:
        """

        source = self.db[source_name]

        logging.info('Dropping', dest_name, 'collection')
        self.db[dest_name].drop()
        # if data exist in the collection drop it

        pipeline = [{"$match": {}},
                    {"$out": dest_name},
                    ]
        logging.info('Copying', source_name, 'collection to', dest_name, 'collection ...')

        source.aggregate(pipeline)
        logging.info('Done')

    def head(self, collection_name, n=10):
        """
        Print n first documents
        :param collection_name:
        :param n:
        :return:
        """

        # try to bring a cursor from a collection
        cursor = self.get_collection(collection_name).find({}).limit(n)
        print("Total documents:" + str(cursor.count()))
        for c in cursor:
            print(c)

    def get_keys(self, collection_name=None):
        """
        Show keys in collection
        :param collection_name:
        :return:
        """

        if not self.collection_exists(self.collection_name):
            raise Exception("Collection {collection_name} not exist".format(collection_name=collection_name))

        source = None
        if collection_name is None:
            source = self.get_collection(self.collection_name)

        results = source.aggregate([
            {"$project": {"arrayofkeyvalue": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$arrayofkeyvalue"},
            {"$group": {"_id": None, "allkeys": {"$addToSet": "$arrayofkeyvalue.k"}}}
        ])

        results = list(results)[0]['allkeys']
        return results

    def show_collections(self, db):
        """
        Show collections in a database
        :return:
        """
        return self.client[db].collection_names()

        # d = dict((db, [collection for collection in self.client[db].collection_names()])
        #         for db in self.client.list_database_names())
        # print(json.dumps(d))

    @staticmethod
    def drop_keys(collection_name, keys):
        """
        Drop key in collection
        :return:
        """
        for key in tqdm_notebook(keys, desc='Processing cols'):
            logging.info('Dropping', key, 'field')
            collection_name.update_many({}, {'$unset': {key: 1}})

    def drop_collection(self, collection_name):
        """
        Drop  a collection
        :param collection_name:
        :return:
        """
        if collection_name is None:
            collection_name = self.collection_name
        self.get_collection(collection_name).drop()

    def save_to_csv(self, filename, collection_name=None, projection=None, limit=0):
        """
        Save collection to csv
        :param filename: Output filename
        :param collection_name: custom collection to save
        :param projection: Filter the keys on csv output
        :param limit: Limit the number to record in the output file
        :return:
        """

        if collection_name is None:
            collection_name = self.collection_name

        collection = self.get_collection(collection_name)

        try:
            file = open(filename, "w", newline='')
            csv_write = csv.writer(file, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        except IOError:
            raise Exception("Could not read file {filename}".format(filename=filename))

        if projection is None:
            projection = {}
        projection["_id"] = 0

        try:
            # Save csv header
            for header in collection.find({}, projection).limit(1):
                csv_write.writerow(header.keys())

            # Save csv body
            documents = collection.find({}, projection).limit(limit)
            count = documents.count(True)

            # Save csv body
            for document in tqdm_notebook(documents, total=count, desc='Processing records'):
                # Get a json, transform it to str and return a semicolon separated string

                result = list(map((lambda x: str(x)), document.values()))
                csv_write.writerow(result)
        except IOError:
            raise Exception("Could not write in {filename}".format(filename=filename))

        file.close()

        # Empty the collecion
        self.flush()

    # CSV https: // gist.github.com / jxub / f722e0856ed461bf711684b0960c8458

    def save_to_json(self, filename, projection=None, limit=0):
        """
        Save collection to json file
        :param filename:
        :param projection:
        :param limit:
        :return:
        """

        _collection = self.get_collection(self.collection_name)

        file = open(filename, "w")
        file.write('[')

        i = 0

        # dump all the data
        documents = _collection.find({}, projection).limit(limit)
        count = documents.count(True)

        for r in tqdm_notebook(documents, total=count, desc='Processing records'):

            i = i + 1
            # FIX: we should remove the id in the projection
            r.pop('_id')

            file.write(json.dumps(r))
            if (i < count):
                file.write(',')

        file.write(']')
        file.close()

    def save_to_geojson(self, filename, coordinates_keys, projection=None, limit=0):
        """
        Save collection to geojson file
        :param filename: Output file
        :param coordinates_keys:
        :param projection:
        :return:
        """

        _collection = self.get_collection(self.collection_name)

        file = open(filename, "w")
        file.write('{"type": "FeatureCollection","features":[')

        i = 0

        # dump all the data
        projection = coordinates_keys + projection
        documents = _collection.find({}, projection).limit(limit)
        count = documents.count(True)

        for r in tqdm_notebook(documents, total=count, desc='Processing records'):

            i = i + 1

            lon_key = coordinates_keys[0]
            # Verify if the key exist and is a float number
            if (lon_key in r) and (isinstance(r[lon_key], float)):
                lon = r[lon_key]

            lat_key = coordinates_keys[1]
            if (lat_key in r) and (isinstance(r[lat_key], float)):
                lat = r[lat_key]

            # FIX: we should remove the id in the projection
            r.pop('_id')
            r.pop(lon_key)
            r.pop(lat_key)

            features = {"type": "Feature", "properties": r, "geometry": {"type": "Point", "coordinates": [lon, lat]}}

            file.write(json.dumps(features))
            if (i < count):
                file.write(',')

        file.write(']}')
        file.close()

    # FIX: not work need to find how to implement in a cursor object
    def head_cursor(self, collection_name_or_cursor, n=1):
        """

        :param collection_name_or_cursor:
        :param n:
        :return:
        """
        if not int(n): raise Exception('n must be an integer')

        if isinstance(collection_name_or_cursor, str):
            # try to bring a cursor from a collection
            cursor = self.get_collection(collection_name_or_cursor).find({}).limit(n)
            count = cursor.count(True)
        else:
            cursor = collection_name_or_cursor
            cursor.rewind()
            count = n

        # FIX: and elegant way to make it in python?
        i = 0

        for c in cursor:
            if (i < count):
                print(c)
            else:
                break
            i = i + 1

    def insert_to_collection(self, cursor, dest_collection_name, drop=False):
        """
        Insert a cursor into a collection
        :param cursor:
        :param dest_collection_name:
        :param drop:
        :return:
        """
        dest_collection = self.get_collection(dest_collection_name)
        if drop:
            dest_collection.drop()

        if "count" in dir(cursor):
            count = cursor.count()
        else:
            count = 1

        for c in tqdm_notebook(cursor, total=count, desc='Saving Collection'):
            dest_collection.insert_one(c)

    def create_missing_fields(self, cols, collection_name=None):
        """

        :param cols:
        :param collection_name:
        :return:
        """

        if collection_name is not None:
            source = self.get_collection(collection_name)
        else:
            source = self.collection

        for c in tqdm_notebook(cols, total=len(cols), desc='Processing cols'):
            logging.info('Inserting', c)
            if c:
                source.update_many(
                    {c: {'$exists': False}},
                    {'$set':
                        {
                            c: None,
                        }
                    }
                );
            else:
                logging.info('Field', c, 'could not be added')

    def cast(self, collection_name, field, convert_to):
        """

        :param collection_name:
        :param field:
        :param convert_to:
        :return:
        """
        collection = self.get_collection(collection_name)
        cursor = collection.find({field: {'$exists': True}}).limit(0)
        desc = 'Converting', field, 'to', convert_to

        # for c in tqdm_notebook(cursor, total = cursor.count(), desc = 'sad'):

        if convert_to == 'int':
            data_type = float
        elif convert_to == 'float':
            data_type = int
        elif convert_to == 'string':
            data_type = str
        else:
            raise ValueError('Only int, float or string accepted in field param', field, 'value present')

        for c in tqdm_notebook(cursor, total=cursor.count(), desc='Processing records'):
            try:
                val = c[field]
                val = data_type(c[field])
                collection.update_one({'_id': c['_id']}, {'$set': {field: val}})

            except ValueError:
                logging.info('Could not convert "', val, '" to', convert_to)
