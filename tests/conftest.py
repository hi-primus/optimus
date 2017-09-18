import findspark
findspark.init()

import logging
import pytest

from pyspark.sql.session import SparkSession
from pyspark import SparkContext


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
    sc = SparkContext.getOrCreate()
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark session."""

    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark
