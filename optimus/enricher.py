import json
import logging
import urllib

import requests
from pymongo import MongoClient
from tqdm import tqdm_notebook


class Enricher:

    def __init__(self, host, port, db_name=None, collection_name=None):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)

        if self.db_exists(db_name):
            self.db = self.client[db_name]
        else:
            raise Exception('Database do not exist')

        if self.collection_exists(collection_name):
            self.collection = self.client[collection_name]
        else:
            raise Exception('Collection do not exist')

    def run(self):
        """

        :return:
        """
        collection = self.get_collection('step3')
        cursor = collection.find({'$or': [{'lat': None}, {'lng': None}]},
                                 projection=['_id', 'lat', 'lng', 'state', 'city'])

        for r in tqdm_notebook(cursor, total=cursor.count(), desc='Geolocation'):
            state = r['state'].lower()
            state = "".join(state.split())

            if (state == "edo.demexico"):
                state = "estado de mexico"
            else:
                state = r['state']

            url = "http://46.101.10.63:8080/search?format=jsonv2&q=mexico+" + state + '+' + r['city']
            url = urllib.parse.unquote_plus(url)

            result = requests.get(url)
            result_json = json.loads(result.text)
            if (result_json):

                lat = round(float(result_json[0]['lat']), 6)
                lon = round(float(result_json[0]['lon']), 6)
                collection.update_one({'_id': r['_id']}, {'$set': {'lat': lat, 'lng': lon}}, upsert=False)

            else:
                print('No procesado')

    def collection_exists(self, collection_name):
        """
        Check if a collection exist
        :param collection_name:
        :return:
        """
        if collection_name in self.db.collection_names():
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

    def get_collection(self, collection_name):
        """

        :param collection_name:
        :return:
        """
        return self.db[collection_name]

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

    def show_cols(self, collection_name=None):
        """
        Show cols
        :param collection_name:
        :return:
        """

        if not self.collection_exists(collection_name):
            raise Exception("Collection {collection_name} not exist".format(collection_name=collection_name))

        source = None
        if collection_name is not None:
            source = self.get_collection(collection_name)

        results = source.aggregate([
            {"$project": {"arrayofkeyvalue": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$arrayofkeyvalue"},
            {"$group": {"_id": None, "allkeys": {"$addToSet": "$arrayofkeyvalue.k"}}}
        ])
        logging.info(results)
        results = list(results)[0]['allkeys']

        for r in results:
            logging.info(r)

    def get_cols(self, collection_name=None):
        """

        :param collection_name:
        :return:
        """
        if collection_name is not None:
            source = self.get_collection(collection_name)
        else:
            source = self.collection
        logging.info(source)
        logging.info('Getting cols...')
        result = source.aggregate([
            {"$project": {"arrayofkeyvalue": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$arrayofkeyvalue"},
            {"$group": {"_id": None, "allkeys": {"$addToSet": "$arrayofkeyvalue.k"}}}
        ])
        result = list(result)[0]['allkeys']
        logging.info('Done')
        return result

    def drop_col(cols):
        """

        :return:
        """
        for x in tqdm_notebook(cols, desc='Processing cols'):
            logging.info('Dropping', x, 'field')
            dest_collection.update_many({}, {'$unset': {x: 1}})

    def insert_to_collection(self, cursor, dest_collection_name, drop=False):
        """

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

    def convert_field_to(self, collection_name, field, convert_to):
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
            l = float
        elif convert_to == 'float':
            l = int
        elif convert_to == 'string':
            l = str
        else:
            raise ValueError('Only int, float or string accepted in field param', field, 'value present')

        for c in tqdm_notebook(cursor, total=cursor.count(), desc='Processing records'):
            try:
                val = c[field]
                val = l(c[field])
                collection.update_one({'_id': c['_id']}, {'$set': {field: val}})

            except ValueError:
                logging.info('Could not convert "', val, '" to', convert_to)

    @staticmethod
    def mongo_to_json_array_file(filename, host, port, db, collection, projection=None, limit=None):
        if (limit is None):
            limit = 0

        client = MongoClient(host, port)
        _db = client[db]
        _collection = _db[collection]

        file = open(filename, "w")
        file.write('[')

        i = 0
        properties = {}

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

    @staticmethod
    def to_geojson_file(filename, host, port, db, collection, coordinates_keys, projection=None):

        client = MongoClient(host, port)
        _db = client[db]
        _collection = _db[collection]

        file = open(filename, "w")
        file.write('{"type": "FeatureCollection","features":[')

        i = 0
        properties = {}

        # dump all the data
        projection = coordinates_keys + projection
        # rojection.append({'_id':False})
        documents = _collection.find({}, projection).limit(0)
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

    def merge_two_dicts(x, y):
        """

        :param y:
        :return:
        """
        z = x.copy()  # start with x's keys and values
        z.update(y)  # modifies z with y's keys and values & returns None
        return z

    def head(self, collection_name, n=1):
        """

        :param collection_name:
        :param n:
        :return:
        """
        if not int(n):
            raise Exception('n param must be an integer')
        if self.collection_exists(collection_name):
            if isinstance(collection_name, str):
                # try to bring a cursor from a collection
                cursor = self.get_collection(collection_name).find({}).limit(n)
                count = cursor.count(True)
            # FIX: and elegant way to make it in python?
            i = 0

            for c in cursor:
                if (i < count):
                    print(c)
                else:
                    break
                i = i + 1
        else:
            msg = 'Collection', collection_name, ' do not exist'
            raise Exception(msg)

    # FIX: not work need to find how to implement in a cursor object
    def head_cursor(self, collection_name_or_cursor, n=1):
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
