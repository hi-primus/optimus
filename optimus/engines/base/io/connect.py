from optimus.engines.base.dask.io.jdbc import DaskBaseJDBC
from optimus.engines.spark.io.properties import DriverProperties
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import regex_full_url
import re

SCHEMAS = ['s3://','gcs://','gc://','http://','https://','ftp://','file://','az://','adl://','abfs://']

class Connection:
    """
    Generic
    """

    def __init__(self, **kwargs):
        if not kwargs["base_url"].endswith("/"):
            kwargs["base_url"] = kwargs["base_url"] + "/"
        self.options = kwargs


    def path(self, path):
        if self.options["base_url"] and not path.startswith((self.options["base_url"], *SCHEMAS)):
            path = self.options["base_url"] + path

        return path


    @property
    def storage_options(self):
        storage_options = self.options.copy()
        display(self.__class__)
        storage_options.pop("base_url")

        if not len(storage_options.keys()):
            storage_options = None
        else:
            storage_options = {k: storage_options[k] for k in storage_options.keys() if not k.startswith('_')}

        return storage_options

class S3(Connection):
    """
    Amazon S3
    """

    def __init__(self, url=None, bucket=None, **kwargs):
        """
        url: http(s)://<bucket name>.<subdomain>.<domain>
        bucket
        anon: Whether access should be anonymous (default False)
        key, secret: For user authentication
        token: If authentication has been done with some other S3 client
        use_ssl: Whether connections are encrypted and secure (default True)
        client_kwargs: Dict passed to the boto3 client, with keys such as region_name or endpoint_url.
            Notice: do not pass the config option here, please pass it’s content to config_kwargs instead.
        config_kwargs: Dict passed to the s3fs.S3FileSystem, which passes it to the boto3 client’s config option.
        requester_pays: Set True if the authenticated user will assume transfer costs,
            which is required by some providers of bulk data
        default_block_size, default_fill_cache: These are not of particular interest to Dask users,
            as they concern the behaviour of the buffer between successive reads
        kwargs: Other parameters are passed to the boto3 Session object, such as profile_name,
            to pick one of the authentication sections from the configuration files referred to above (see here)


        """
        if url is None:
            RaiseIt.value_error(url, "")

        schema_result = re.search(regex_full_url, url)

        if schema_result is not None:
            schema = schema_result[2]
            url = url[len(schema)+3:] # removes schema from url

        kwargs['client_kwargs'] = kwargs.get('client_kwargs', {})

        schema = schema if schema else 'https'

        kwargs['client_kwargs']['endpoint_url'] = kwargs['client_kwargs'].get('endpoint_url', schema+"://"+url)

        if not kwargs.get('base_url', False):
            if bucket:
                kwargs["base_url"] = 's3://'+bucket
            elif url:
                kwargs["base_url"] = 's3://'+url

        super().__init__(**kwargs)


class Local(Connection):
    """
    Local file system
    """

    def __init__(self, **kwargs):
        """
        supports base_url
        :param kwargs:
        """
        super().__init__(**kwargs)


class MAS(Connection):
    """
    Microsoft Azure Storage
    """

    def __init__(self, **kwargs):
        """
        Authentication for adl requires tenant_id, client_id and client_secret in the storage_options dictionary.
        Authentication for abfs requires account_name and account_key in storage_options.
        :param kwargs:
        """
        super().__init__(**kwargs)


class GCS(Connection):
    def __init__(self, host, port, user, kerb_ticket):
        super().__init__(host, port, user, kerb_ticket)


class HDFS(Connection):
    def __init__(self, host, port, user, kerb_ticket):
        super().__init__(host, port, user, kerb_ticket)

class Connect:
    @staticmethod
    def mysql(host=None, database=None, user=None, password=None, port=None, schema="public"):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.MYSQL.value["name"],
                            schema=schema)

    @staticmethod
    def postgres(host=None, database=None, user=None, password=None, port=None, schema="public"):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.POSTGRESQL.value["name"],
                            schema=schema)

    @staticmethod
    def mssql(host=None, database=None, user=None, password=None, port=None, schema="public"):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.MSSQL.value["name"],
                            schema=schema)

    @staticmethod
    def redshift(host=None, database=None, user=None, password=None, port=None, schema="public"):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.REDSHIFT.value["name"],
                            schema=schema)

    @staticmethod
    def sqlite(host=None, database=None, user=None, password=None, port=None, schema="public"):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.SQLITE.value["name"],
                            schema=schema)

    @staticmethod
    def bigquery(host=None, database=None, user=None, password=None, port=None, schema="public", project=None,
                 dataset=None):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.BIGQUERY.value["name"],
                            schema=schema, bigquery_project=project, bigquery_dataset=dataset)

    @staticmethod
    def presto(host=None, database=None, user=None, password=None, port=None, schema="public", catalog=None):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.PRESTO.value["name"],
                            schema=schema, presto_catalog=catalog, )

    @staticmethod
    def cassandra(host=None, database=None, user=None, password=None, port=None, schema="public", keyspace=None,
                  table=None):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.CASSANDRA.value["name"],
                            schema=schema, cassandra_keyspace=keyspace,
                            cassandra_table=table)

    @staticmethod
    def redis(host=None, database=None, user=None, password=None, port=None, schema="public"):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.REDIS.value["name"],
                            schema=schema)

    @staticmethod
    def oracle(host=None, database=None, user=None, password=None, port=None, schema="public",
               tns=None, service_name=None, sid=None):
        return DaskBaseJDBC(host, database, user, password, port=port, driver=DriverProperties.ORACLE.value["name"],
                            schema=schema, oracle_tns=tns, oracle_service_name=service_name, oracle_sid=sid)

    @staticmethod
    def s3(**kwargs):
        return S3(**kwargs)
    

    @staticmethod
    def local(**kwargs):
        return Local(**kwargs)


    @staticmethod
    def hdfs(**kwargs):
        return HDFS(**kwargs)


    @staticmethod
    def gcs(**kwargs):
        """
        Google Cloud Storage
        :param kwargs:
        :return:
        """
        return GCS(**kwargs)


    @staticmethod
    def mas(**kwargs):
        """
        Microsoft Azure Storage
        :param kwargs:
        :return:
        """
        return MAS(**kwargs)
