from typing import Any, Dict, Union
import unittest


class TestBase(unittest.TestCase):

    config: Dict[str, Union[str, int]]  = {"engine": "pandas", "n_partitions": 1}
    dict: Dict[Any, Any] = {}
    load: Dict[Any, Any]  = {}

    @classmethod
    def setUpClass(cls):

        if not cls.config:
            raise Exception("Please initialize device before running tests")

        import sys
        sys.path.append("..")
        from optimus import Optimus
        cls.op = Optimus(cls.config["engine"])
        print(f"optimus using {cls.op.engine}")

        if cls.dict:
            cls.df = cls.create_dataframe(cls.dict, force_data_types=True)
        elif cls.load:
            if isinstance(cls.load, str):
                cls.df = cls.load_dataframe(cls.load)
            else:
                cls.df = cls.load_dataframe(**cls.load)
        cls.post_create()

    @classmethod
    def create_dataframe(cls, data=None, **kwargs):
        if not "n_partitions" in kwargs and "n_partitions" in cls.config:
            kwargs["n_partitions"] = cls.config["n_partitions"]

        return cls.op.create.dataframe(data, **kwargs)

    @classmethod
    def load_dataframe(cls, path=None, type='file', **kwargs):
        if not "n_partitions" in kwargs and "n_partitions" in cls.config:
            kwargs["n_partitions"] = cls.config["n_partitions"]

        return getattr(cls.op.load, type)(path, **kwargs)

    @classmethod
    def post_create(cls):
        pass
