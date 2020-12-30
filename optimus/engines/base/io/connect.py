from optimus.engines.base.dask.io.jdbc import DaskBaseJDBC
from optimus.engines.spark.io.properties import DriverProperties
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import regex_full_url
import re

class S3:
    """
    Amazon S3
    """

    def __init__(self, url=None, **kwargs):
        """
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


        schema = re.search(regex_full_url, url)[2]
        if schema is None:
            self.url = "s3:" + url

        self.options = kwargs


class MAS:
    """
    Microsoft Azure Storage
    """

    def __init__(self, **kwargs):
        """
        Authentication for adl requires tenant_id, client_id and client_secret in the storage_options dictionary.
        Authentication for abfs requires account_name and account_key in storage_options.
        :param kwargs:
        """


class HDFS:
    def __init__(self, host, port, user, keb_ticket):
        self.options = args

    def options(self):
        return self.options


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
    def hdfs(**kwargs):
        return HDFS(**kwargs)

    @staticmethod
    def gcs(**kwargs):
        """
        Google Cloud Storage
        :param kwargs:
        :return:
        """
        return HDFS(**kwargs)

    @staticmethod
    def mas(**kwargs):
        """
        Microsoft Azure Storage
        :param kwargs:
        :return:
        """
        return MAS(**kwargs)
