import findspark
findspark.init()

import pytest

from pyspark.sql.session import SparkSession
from pyspark import SparkContext


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    sc = SparkContext.getOrCreate()
    request.addfinalizer(lambda: sc.stop())

    return sc


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark session."""

    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    return spark
