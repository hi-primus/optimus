def test_spark_context_fixture(spark_context):
    test_rdd = spark_context.parallelize([1, 2, 3, 4])

    assert test_rdd.count() == 4
