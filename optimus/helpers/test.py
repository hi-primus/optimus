import errno
from optimus.api.code import generate_code
import warnings
import os
from io import UnsupportedOperation
from pprint import pformat

from optimus.engines.base.basedataframe import BaseDataFrame

from optimus.infer import is_function, is_list_value, is_list_empty, is_list_of_str, is_list_of_numeric, is_list_of_tuples, \
    is_numeric, is_str, is_dict
from optimus.helpers.debug import get_var_name
from optimus.helpers.logger import logger


class Test:
    def __init__(self, op=None, df=None, name=None, path=None, final_path=None, options={}, **kwargs):
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
        self.op = op
        self.df = df
        self.name = name
        self.path = path
        self.final_path = final_path
        self.options = options if options != {} else kwargs

    def run(self):
        """
        Return the tests in text format
        :return:
        """

        final_path = self.final_path
        if self.path is None:
            filename = "test_" + self.name + ".py"
        else:
            filename = final_path + "/" + "test_" + self.name + ".py"

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

        for root, dirs, files in os.walk(self.path):
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

        test_file.write("\nif __name__ == '__main__': unittest.main()")

        test_file.close()
        print("Done")

    def create(self, obj, method, suffix=None, output="df", additional_method=None, *args, **kwargs):
        """
        This is a helper function that output python tests for Spark DataFrames.
        :param obj: Object to be tested
        :param method: Method to be tested
        :param suffix: The test name will be create using the method param. suffix will add a string in case you want
        to customize the test name.
        :param output: can be a 'df' or a 'json'
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

        if additional_method is not None:
            name.append(additional_method)

        if suffix is not None:
            name.append(suffix)

        test_name = "_".join(name)

        func_test_name = "test_" + test_name + "()"

        print("Creating {test} test function...".format(test=func_test_name))
        logger.print(func_test_name)

        if not output == "dict":
            add_buffer("@staticmethod\n")
            func_test_name = "test_" + test_name + "()"
        else:
            func_test_name = "test_" + test_name + "(self)"

        filename = test_name + ".test"

        add_buffer("\n")
        add_buffer("def " + func_test_name + ":\n")

        source = "source_df"

        n_partitions = self.options.get("n_partitions", 1)

        if obj is None:
            # Use the main df
            df_func = self.df
        elif isinstance(obj, (BaseDataFrame,)):

            source_df = "    " + generate_code(source="op", target="source_df",
                                             operation="create.dataframe", dict=obj.export(), n_partitions=n_partitions) + "\n"

            df_func = obj
            add_buffer(source_df)
        else:
            source = get_var_name(obj)
            df_func = obj

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

            # if is_str(v):
            #     _args.append("'" + v + "'")
            # elif is_numeric(v):
            #     _args.append(str(v))

            # elif is_list_value(v):
            #     if is_list_of_str(v):
            #         lst = ["'" + x + "'" for x in v]
            #     elif is_list_of_numeric(v) or is_list_of_tuples(v):
            #         lst = [str(x) for x in v]
            #     elif is_list_of_tuples(v):
            #         lst = [str(x) for x in v]
            #     _args.append('[' + ','.join(lst) + ']')
            # elif is_dict(v):
            #     _args.append(json.dumps(v))
            # elif is_function(v):
            #     _args.append(v.__qualname__)
            # elif is_dask_dataframe(v):
            #     _args.append("op.create.df('"+v.export()+"')")
            # else:
            #     _args.append(str(v))

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
            am = ""
            if additional_method:
                am = "." + additional_method + "()"

            add_buffer("    actual_df = " + source + "." + method + "(" + _args + separator + ','.join(
                _kwargs) + ")" + am + "\n")

        # print("df_result", df_result)
        # Apply function to the spark
        if method is None:
            df_result = self.op.create.dataframe(
                *args, **kwargs, n_partitions=n_partitions)
        else:
            # Here we construct the method to be applied to the source object
            for f in method.split("."):
                df_func = getattr(df_func, f)

            df_result = df_func(*args, **kwargs)

        # Additional Methods
        if additional_method is not None:
            df_result = getattr(df_result, additional_method)()
        if output == "df":
            df_result.table()
            expected = "    " + \
                generate_code(source="op", target="expected_df", operation="create.dataframe", dict=df_result.export(), n_partitions=n_partitions) + \
                "\n"

        elif output == "json":
            if is_str(df_result):
                df_result = "'" + df_result + "'"
            else:
                df_result = str(df_result)
            add_buffer("    actual_df = json_enconding(actual_df)\n")
            expected = "    expected_value = json_enconding(" + df_result + ")\n"

        elif output == "dict":
            expected = "    expected_value =" + str(df_result) + "\n"
        else:
            expected = "    \n"

        add_buffer(expected)

        # Output
        if output == "df":
            add_buffer("    self.assertTrue(expected_df.equals(actual_df))\n")
        elif output == "json":
            add_buffer("    self.assertTrue(expected_value == actual_df)\n")
        elif output == "dict":
            add_buffer("    self.assertDictEqual(deep_sort(expected_value),  deep_sort(actual_df))\n")

        filename = self.path + "//" + filename
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

    def delete(self, df, func, suffix=None, *args, **kwargs):
        """
        This is a helper function that delete python tests files used to construct the final Test file.
        :param df: Do nothing, only for simplicity so you can delete a file test the same way you create it
        :param suffix: The create method will try to create a test function with the func param given.
        If you want to test a function with different params you can use suffix.
        :param func: Function to be tested
        :return:
        """

        if suffix is None:
            suffix = ""
        else:
            suffix = "_" + suffix

        # Create func test name. If is None we just test the create.df function a not transform the data frame in
        # any way
        if func is None:
            func_test_name = "test_" + "create_df" + suffix + "()"
            filename = "create_df" + suffix + ".test"

        else:
            func_test_name = "test_" + func.replace(".", "_") + suffix + "()"

            filename = func.replace(".", "_") + suffix + ".test"

        filename = self.path + "//" + filename

        print("Deleting file {test}...".format(test=filename))
        logger.print(func_test_name)
        try:
            os.remove(filename)
        except FileNotFoundError as e:
            warnings.warn(e)
