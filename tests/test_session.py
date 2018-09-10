import sys
import unittest

try:
    from unittest import mock
except ImportError:
    import mock

from pyspark import SparkContext

from optimus import Optimus


class TestOptimusSession(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        super(TestOptimusSession, self).setUp()
        self.spark_context_mock = mock.Mock(spec=SparkContext)

        self.patches = [
            mock.patch('optimus.sc', self.spark_context_mock),
        ]
        [p.start() for p in self.patches]

    def tearDown(self):
        [p.stop() for p in self.patches]
        super(TestOptimusSession, self).tearDown()

    def test_has_package(self):
        op = Optimus()
        self.assertFalse(op.has_package('datastax:spark-cassandra-connector'))

        op.packages = ['datastax:spark-cassandra-connector:1.6.1-s_2.10']
        self.assertTrue(op.has_package('datastax:spark-cassandra-connector'))

    def test_has_jar(self):
        op = Optimus()
        self.assertFalse(op.has_jar('mysql-connector-java'))

        op.jars = ['mysql-connector-java-5.1.39-bin.jar']
        self.assertTrue(op.has_jar('mysql-connector-java'))

    @mock.patch('optimus.os')
    def test_session_with_packages(self, os_mock):
        os_mock.environ = {}

        Optimus(packages=['package1', 'package2'])

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--packages package1,package2 '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    @mock.patch('optimus.os')
    def test_session_with_repositories(self, os_mock):
        os_mock.environ = {}

        Optimus(
            packages=['package1', 'package2'],
            repositories=[
                'http://my.maven.repo',
                'http://another.maven.repo',
            ])

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--repositories http://my.maven.repo,http://another.maven.repo '
                '--packages package1,package2 '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    @mock.patch('optimus.os')
    def test_session_with_jars(self, os_mock):
        os_mock.environ = {}

        Optimus(jars=['file_a.jar', 'file_b.jar'])

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--jars file_a.jar,file_b.jar '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    @mock.patch('optimus.os')
    def test_session_with_options(self, os_mock):
        os_mock.environ = {}

        # test options attached to class definition
        Optimus(
            options={
                'spark.option.a': 'value_a',
                'spark.option.b': 'value_b',
            })

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "spark.option.a=value_a" '
                '--conf "spark.option.b=value_b" '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

        # test additional_options override/extend options attached to class definition
        os_mock.environ = {}

        Optimus(additional_options={
            'spark.option.b': 'value_0',
            'spark.option.c': 'value_c',
        })

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "spark.option.a=value_a" '
                '--conf "spark.option.b=value_0" '
                '--conf "spark.option.c=value_c" '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

        # test catalog implementation is respected
        os_mock.environ = {}

        Optimus(options={
            'spark.sql.catalogImplementation': 'my_fancy_catalog',
        })

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "spark.sql.catalogImplementation=my_fancy_catalog" '
                'pyspark-shell'
            ),
        })

    @mock.patch('optimus.os')
    def test_session_without_packages_jars_and_options(self, os_mock):
        os_mock.environ = {}

        Optimus()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': '--conf "spark.sql.catalogImplementation=hive" pyspark-shell',
        })

    @mock.patch('optimus.os')
    def test_session_appends_to_pyspark_submit_args(self, os_mock):
        os_mock.environ = {
            'PYSPARK_SUBMIT_ARGS': '--conf "my.conf.here=5g" --and-other-properties',
        }

        Optimus()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "my.conf.here=5g" --and-other-properties '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

        # test more complicated session
        os_mock.environ = {
            'PYSPARK_SUBMIT_ARGS': '--conf "my.conf.here=5g" --and-other-properties',
        }

        Optimus(options={'my.conf.here': '10g'})

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "my.conf.here=5g" --and-other-properties '
                # Note that spark honors the first conf it sees when multiple
                # are defined
                '--conf "my.conf.here=10g" '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })


