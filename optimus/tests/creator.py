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

    created = []

    def __init__(self, op=None, df=None, name=None, path="", create_path="..", configs={}, **kwargs):
        """
        Create python code with unit test functions for Optimus.
        :param op: optimus instance
        :param df: Spark Dataframe
        :param name: Name of the Test Class
        :type path: folder where tests will be written individually. run() nneds to be runned to merge all the tests.
        :param options: dictionary with configuration options
            import: Libraries to be added. 
        :param configs: Array of dictionaries with:
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
        self.configs = configs

    def run(self, clear=True):
        """
        Return the tests in text format
        :return:
        """

        filename = self.create_path + "/" + "test_" + self.name + ".py"

        test_file = open(filename, 'w', encoding='utf-8')
        print("Creating file " + filename)

        # Imports
        _imports = [
            "from optimus.tests.base import TestBase",
            "import datetime",
            "nan = float(\"nan\")",
            "inf = float(\"inf\")",
            "from optimus.helpers.json import json_encoding",
            "from optimus.helpers.functions import deep_sort"
        ]

        if self.options.get("imports", None) is not None:
            for i in self.options["imports"]:
                _imports.append(i)
        for i in _imports:
            test_file.write(i + "\n")

        classes = {}

        for name, config in self.configs.items():
            key = "".join(w.title() for w in self.name.split("_")) + name
            classes.update({key: config})

        # Class name
        cls = "\nclass Test" + list(classes.keys())[0] + "(TestBase):\n"
        test_file.write(cls)

        # First Config
        test_file.write("    config = " +
                        pformat(list(classes.values())[0])+"\n")

        # Global Dataframe
        if self.df is not None:
            source_df = "    dict = " + self.df.export() + "\n"
            test_file.write(source_df)

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

        for name, config in list(classes.items())[1:]:

            cls = "\nclass Test" + name + "(TestBase):\n"
            test_file.write(cls)

            test_file.write("    config = " + pformat(config)+"\n")

        test_file.close()

        if clear:
            self.clear()

        print("Done")

    def clear(self):
        for method, variant in self.created:
            self.delete(method, variant)

    def create(self, method=None, variant=None, df=None, compare_by="dict", additional_method=[], args=(), **kwargs):
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

        for a_method in additional_method:
            name.append(a_method)

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

        source = "self.df"

        if df is None:
            # Use the main df
            df_func = self.df
        elif isinstance(df, (BaseDataFrame,)):
            source_df = "    self.create_dataframe(dict=" + df.export() + ")\n"
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
                _df = "    self.create_dataframe(dict=" + v.export() + ")\n"
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
            add_buffer("    result = self.df\n")
        else:
            ams = ""
            for method in additional_method:
                ams += "." + method + "()"

            add_buffer("    result = " + source + "." + method + "(" + _args + separator + ','.join(
                _kwargs) + ")" + ams + "\n")

        # print("df_result", df_result)
        # Apply function
        if method is None:
            df_result = self.op.create.dataframe(*args, **kwargs)
        else:
            # Here we construct the method to be applied to the source object
            for f in method.split("."):
                df_func = getattr(df_func, f)

            df_result = df_func(*args, **kwargs)

        # Additional Methods
        for a_method in additional_method:
            df_result = getattr(df_result, a_method)()

        # Checks if output is ok
        if not isinstance(df_result, (BaseDataFrame,)):
            if compare_by == "df":
                compare_by = "json"
        else:
            df_result = df_result.export(dtypes=False)
            add_buffer("    result = result.to_dict()\n")

        if compare_by == "df":
            expected = "    expected = self.create_dataframe(dict=" + \
                df_result + ")\n"

        else:
            expected = "    expected = " + df_result + "\n"

        add_buffer(expected)

        # Output
        if compare_by == "df":
            add_buffer("    self.assertTrue(result.equals(expected))\n")
        elif compare_by == "dict":
            add_buffer(
                "    self.assertDictEqual(deep_sort(result), deep_sort(expected))\n")
        elif compare_by == "json":
            add_buffer(
                "    self.assertEqual(json_encoding(result), json_encoding(expected))\n")
        else:
            add_buffer("    self.assertEqual(result, expected)\n")

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

        self.created.append((method, variant))

        # return "".join(buffer)

    def delete(self, method, variant=None):
        """
        This is a helper function that delete python tests files used to construct the final Test file.
        :param df: Do nothing, only for simplicity so you can delete a file test the same way you create it
        :param variant: The create method will try to create a test function with the func param given.
        If you want to test a function with different params you can use variant.
        :param method: Method to be tested
        :param method: Variant of the method
        :return:
        """

        if variant is None:
            variant = ""
        else:
            variant = "_" + variant

        # Create func test name. If is None we just test the create.df function a not transform the data frame in
        # any way
        if method is None:
            method_test_name = "test_" + "create_df" + variant + "()"
            filename = "create_df" + variant + ".test"

        else:
            method_test_name = "test_" + \
                method.replace(".", "_") + variant + "()"

            filename = method.replace(".", "_") + variant + ".test"

        filename = self.create_path + "/" + filename

        print(f"Deleting file {filename}...")
        logger.print(method_test_name)
        try:
            os.remove(filename)
        except FileNotFoundError as e:
            warnings.warn(e)
