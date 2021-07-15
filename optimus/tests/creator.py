import errno
from optimus.helpers.core import val_to_list
from optimus.api.code import generate_code
import warnings
import os
from io import UnsupportedOperation
from pprint import pformat

from optimus.engines.base.basedataframe import BaseDataFrame

from optimus.infer import is_function, is_list_empty
from optimus.helpers.debug import get_var_name
from optimus.helpers.logger import logger


class TestCreator:
    def __init__(self, op=None, df=None, name=None, path="", create_path="..", configs={}, **kwargs):
        """
        Create python code with unit test functions for Optimus.
        :param op: optimus instance
        :param df: Spark Dataframe
        :param name: Name of the Test Class
        :type path: folder where tests will be written individually. run() nneds to be runned to merge all the tests.
        :param options: dictionary with configuration options
            import: Libraries to be added. 
            engine: Engine to use. 
            n_partitions: Number of partitions of created dataframes (if supported)

        """
        if path and len(path):
            create_path += f"/{path}"

        self.op = op
        self.df = df
        self.name = name
        self.path = path
        self.create_path = create_path
        self.options = kwargs

    def run(self):
        """
        Return the tests in text format
        :return:
        """

        filename = self.create_path + "/" + "test_" + self.name + ".py"

        test_file = open(filename, 'w', encoding='utf-8')
        print("Creating file " + filename)

        # Imports
        _imports = [
            "import sys",
            "sys.path.append(\"..\")",
            "import numpy as np",
            "NaN = np.nan",
            "null = None",
            "false = False",
            "true = True",
            "from optimus import Optimus",
            "from optimus.helpers.json import json_enconding",
            "from optimus.helpers.functions import deep_sort",
            "import unittest"
        ]

        if self.options.get("imports", None) is not None:
            for i in self.options["imports"]:
                _imports.append(i)
        for i in _imports:
            test_file.write(i + "\n")

        # Engine
        engine = self.options.get("engine", "pandas")

        # Number of partitions
        n_partitions = self.options.get("n_partitions", 1)

        # Creating the Optimus instance
        test_file.write(f"op = Optimus(\"{engine}\")\n")

        # Global Dataframe
        if self.df is not None:
            source_df = generate_code(source="op", target="source_df",
                                      operation="create.dataframe", dict=self.df.export(), n_partitions=n_partitions) + "\n"
            test_file.write(source_df)

        # Class name
        cls = "\nclass Test_" + self.name + "(unittest.TestCase):\n"

        test_file.write(cls)
        test_file.write("    maxDiff = None\n")

        for root, dirs, files in os.walk(self.create_path):
            for file in files:
                if file.endswith(".test"):
                    full_path = os.path.join(root, file)

                    with open(full_path, 'r', encoding='utf-8') as opened_file:
                        try:
                            text = opened_file.read()

                            test_file.write(text)
                            opened_file.close()
                        except UnsupportedOperation:
                            print("file seems to be empty")

        # test_file.write("\nif __name__ == '__main__': unittest.main()")

        test_file.close()
        print("Done")

    def create(self, method=None, variant=None, df=None, compare_by="df", additional_method=[], args=(), **kwargs):
        """
        This is a helper function that output python tests for Spark DataFrames.
        :param method: Method to be tested
        :param variant: The test name will be create using the method param. This will be added as a suffix in case you want to customize the test name.
        :param df: Object to be tested
        :param compare_by: 'df', 'json' or 'dict'
        :param additional_method:
        :param args: Arguments to be used in the method
        :param kwargs: Keyword arguments to be used in the functions
        :return:
        """

        buffer = []

        def add_buffer(value):
            buffer.append("    " + value)

        # Create name
        name = []

        if method is not None:
            name.append(method.replace(".", "_"))

        additional_method = val_to_list(additional_method)

        for additional_method in additional_method:
            name.append(additional_method)

        if variant is not None:
            name.append(variant)

        test_name = "_".join(name)

        func_test_name = "test_" + test_name + "()"

        print("Creating {test} test function...".format(test=func_test_name))
        logger.print(func_test_name)

        func_test_name = "test_" + test_name + "(self)"

        filename = test_name + ".test"

        add_buffer("\n")
        add_buffer("def " + func_test_name + ":\n")

        source = "source_df"

        n_partitions = self.options.get("n_partitions", 1)

        if df is None:
            # Use the main df
            df_func = self.df
        elif isinstance(df, (BaseDataFrame,)):
            source_df = "    " + generate_code(source="op", target="source_df",
                                               operation="create.dataframe", dict=df.export(), n_partitions=n_partitions) + "\n"
            df_func = df
            add_buffer(source_df)
        else:
            source = get_var_name(df)
            df_func = df

        # Process simple arguments
        _args = []
        for v in args:
            if is_function(v):
                _args.append(v.__qualname__)
            elif isinstance(v, (BaseDataFrame,)):
                _df = generate_code(source="op", target=False, operation="create.dataframe", dict=v.export(),
                                    n_partitions=n_partitions)
                _args.append(_df)
            elif isinstance(v, (str, bool, dict, list)):
                _args.append(pformat(v))
            else:
                _args.append(str(v))

        _args = ','.join(_args)
        _kwargs = []

        # Process keywords arguments
        for k, v in kwargs.items():
            _kwargs.append(k + "=" + pformat(v))

        # Separator if we have positional and keyword arguments
        separator = ""
        if (not is_list_empty(args)) & (not is_list_empty(kwargs)):
            separator = ","

        if method is None:
            add_buffer("    actual_df = source_df\n")
        else:
            ams = ""
            for method in additional_method:
                ams += "." + method + "()"

            add_buffer("    actual_df = " + source + "." + method + "(" + _args + separator + ','.join(
                _kwargs) + ")" + ams + "\n")

        # print("df_result", df_result)
        # Apply function
        if method is None:
            df_result = self.op.create.dataframe(
                *args, **kwargs, n_partitions=n_partitions)
        else:
            # Here we construct the method to be applied to the source object
            for f in method.split("."):
                df_func = getattr(df_func, f)

            df_result = df_func(*args, **kwargs)

        if not isinstance(df_result, (BaseDataFrame,)) and compare_by == "df":
            compare_by = "json"

        # Additional Methods
        for method in additional_method:
            df_result = getattr(df_result, method)()
        if compare_by == "df":
            expected = "    " + \
                generate_code(source="op", target="expected_df", operation="create.dataframe", dict=df_result.export(), n_partitions=n_partitions) + \
                "\n"

        elif compare_by == "json":
            df_result = pformat(df_result)
            add_buffer("    actual_df = json_enconding(actual_df)\n")
            expected = "    expected_value = json_enconding(" + \
                df_result + ")\n"

        elif compare_by == "dict":
            expected = "    expected_value =" + pformat(df_result) + "\n"
        else:
            expected = "    expected_value =" + pformat(df_result) + "\n"

        add_buffer(expected)

        # Output
        if compare_by == "df":
            add_buffer("    self.assertTrue(expected_df.equals(actual_df))\n")
        elif compare_by == "dict":
            add_buffer(
                "    self.assertDictEqual(deep_sort(expected_value),  deep_sort(actual_df))\n")
        else:
            add_buffer("    self.assertEqual(expected_value, actual_df)\n")

        filename = self.create_path + "/" + filename
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        # Write file
        test_file = open(filename, 'w', encoding='utf-8')

        for b in buffer:
            test_file.write(b)

        # return "".join(buffer)

    def delete(self, func, variant=None, *args, **kwargs):
        """
        This is a helper function that delete python tests files used to construct the final Test file.
        :param df: Do nothing, only for simplicity so you can delete a file test the same way you create it
        :param variant: The create method will try to create a test function with the func param given.
        If you want to test a function with different params you can use variant.
        :param func: Function to be tested
        :return:
        """

        if variant is None:
            variant = ""
        else:
            variant = "_" + variant

        # Create func test name. If is None we just test the create.df function a not transform the data frame in
        # any way
        if func is None:
            func_test_name = "test_" + "create_df" + variant + "()"
            filename = "create_df" + variant + ".test"

        else:
            func_test_name = "test_" + func.replace(".", "_") + variant + "()"

            filename = func.replace(".", "_") + variant + ".test"

        filename = self.path + "/" + filename

        print("Deleting file {test}...".format(test=filename))
        logger.print(func_test_name)
        try:
            os.remove(filename)
        except FileNotFoundError as e:
            warnings.warn(e)
