import unittest
import unittest

class TestApi(unittest.TestCase):

    def test_default_engine_configuration(self):
        from optimus.server import Session
        
        s1 = Session(engineConfiguration="pandas")
        s2 = Session(engineConfiguration={"engine": "pandas"})
        s3 = Session({"engineConfiguration": "pandas"})
        s4 = Session({"engineConfiguration": {"engine": "pandas"}})

        self.assertEqual(s1.config["engineConfiguration"], "pandas")
        self.assertEqual(s2.config["engineConfiguration"], {"engine": "pandas"})
        self.assertEqual(s3.config["engineConfiguration"], "pandas")
        self.assertEqual(s4.config["engineConfiguration"], {"engine": "pandas"})

    def test_default_engine_configuration_initialized(self):
        from optimus.server import Session
        
        s1 = Session(engineConfiguration="pandas")
        s2 = Session(engineConfiguration={"engine": "pandas"})
        s3 = Session({"engineConfiguration": "pandas"})
        s4 = Session({"engineConfiguration": {"engine": "pandas"}})

        self.assertEqual(s1.default_engine.name, "op")
        self.assertEqual(s2.default_engine.name, "op")
        self.assertEqual(s3.default_engine.name, "op")
        self.assertEqual(s4.default_engine.name, "op")

    def test_engine_configuration(self):
        from optimus.server import Session
        
        session = Session()

        e1 = session.engine("pandas")
        e2 = session.engine({"engine": "pandas"})

        self.assertEqual(e1.name, "op")
        self.assertEqual(e2.name, "op2")

    def test_session_request(self):
        from optimus.server import Session
        
        session = Session()
        response = session.request(operation="createDataframe", dict={"foo": [1, 2.0, "bar"]})

        self.assertEqual(response["updated"], "df")

    def test_engine_request(self):
        from optimus.server import Session
        
        session = Session()

        e1 = session.engine("pandas")
        e2 = session.engine({"engine": "pandas"})

        r1 = e1.request(operation="createDataframe", dict={"foo": [1, 2.0, "bar"]})
        r2 = e2.request(operation="createDataframe", dict={"zoo": [1.0, 2, "baz"]})

        self.assertEqual(r1["updated"], "df")
        self.assertEqual(r2["updated"], "df2")

    def test_direct_request(self):
        from optimus.server.functions import run
        
        response = run("default", dict(source="op", operation="createDataframe", dict={"foo": [1, 2.0, "bar"]}))

        self.assertEqual(response["updated"], "df")
