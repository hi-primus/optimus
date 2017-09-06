import findspark
findspark.init()

import logging
import pytest

from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[*]"))
    sc = SparkContext(conf=conf).getOrCreate()
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc


@pytest.fixture(scope="session")
def hive_context():
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Returns:
        HiveContext for tests
    """
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)
