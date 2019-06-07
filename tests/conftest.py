import findspark

findspark.init()

import pytest

from pyspark.sql.session import SparkSession
from pyspark import SparkContext


@pytest.fixture(scope="session")
def spark_context():
    """ fixture for creating a spark context"""

    spark_context = SparkContext.getOrCreate()

    yield spark_context


@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a spark session."""

    spark_session = (SparkSession
             .builder
             .enableHiveSupport()
             .getOrCreate())

    yield spark_session


def pytest_assertrepr_compare(config, op, left, right):
    if op in ('==', '!='):
        return ['{0} {1} {2}'.format(left, op, right)]
