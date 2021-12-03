import errno
from optimus.helpers.core import val_to_list
import warnings
import os
from io import UnsupportedOperation
from pprint import pformat

from optimus.engines.base.basedataframe import BaseDataFrame

from optimus.infer import is_str, is_function, is_list_empty
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
        self.created = []

    def run(self, clear=True):
        """
        Return the tests in text format
        :return:
        """

        filename = self.create_path + "/" + "test_created__" + self.name + ".py"

        test_file = open(filename, 'w', encoding='utf-8')
        print("Creating file " + filename)

        # Imports
        _imports = [
            "import datetime",
            "import numpy as np",
            "from optimus.tests.base import TestBase",
            "from optimus.helpers.json import json_encoding",
            "from optimus.helpers.functions import deep_sort, df_dicts_equal, results_equal",
            "\n\ndef Timestamp(t):\n    return datetime.datetime.strptime(t, \"%Y-%m-%d %H:%M:%S\")\n\n",
            "NaT = np.datetime64('NaT')",
            "nan = float(\"nan\")",
            "inf = float(\"inf\")",
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
        base_class = "Test"+list(classes.keys())[0]
        cls = "\n\nclass " + base_class + "(TestBase):\n"
        test_file.write(cls)

        # First Config
        test_file.write("    config = " +
                        pformat(list(classes.values())[0], compact=True) + "\n")

        # Global Dataframe
        if self.df is not None:
            test_file.write("    dict = " + self.df.export(data_types="internal") + "\n")

        test_file.write("    maxDiff = None\n")

        for root, dirs, files in os.walk(self.create_path):
            files.sort()
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

        for name, config in list(classes.items())[1:]:

            class_config = config.copy()

            try_package = class_config.pop("try_package", None)

            test_file.write("\n\n")
            extra_tab = ""
            if try_package:
                extra_tab = "    "
                test_file.write("try:\n")
                try_package = val_to_list(try_package)
                for package in try_package:
                    test_file.write(f"    import {package} # pyright: reportMissingImports=false\n")
                test_file.write("except:\n")
                test_file.write("    pass\n")
                test_file.write("else:\n")
                

            test_file.write(f"{extra_tab}class Test{name}({base_class}):\n")
            test_file.write(f"{extra_tab}    config = {pformat(class_config, compact=True)}\n")

        test_file.close()

        if clear:
            self.clear()

        print("Done")

    def clear(self):
        for method, variant in self.created:
            self.delete(method, variant)

    def create(self, method=None, variant=None, df=None, compare_by="df", select_cols=False, additional_method=[], args=(), **kwargs):
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

        def add_buffer(value, tab=1):
            buffer.append(("    " * tab) + value)

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

        print(f"Creating {func_test_name} test function...")
        logger.print(func_test_name)

        func_test_name = "test_" + test_name + "(self)"

        filename = test_name + ".test"

        add_buffer("\n", 0)
        add_buffer("def " + func_test_name + ":\n")

        if select_cols is True:
            select_cols = kwargs["cols"] if "cols" in kwargs else args[0] if len(
                args) else False

        if df is None and self.df is None and (len(args)+len(kwargs)):
            df = self.op.create.dataframe(*args, **kwargs)
            df_func = df

        if df is None:
            # Use the main df
            df = self.df
            if select_cols:
                df = df.cols.select(select_cols)
                add_buffer(
                    f"df = self.df.copy().cols.select({pformat(select_cols, compact=True)})\n", 2)
            else:
                add_buffer("df = self.df.copy()\n", 2)
            df_func = df
        elif isinstance(df, (BaseDataFrame,)):
            if select_cols:
                df = df.cols.select(select_cols)
            add_buffer("df = self.create_dataframe(data=" + df.export(data_types="internal") + ", force_data_types=True)\n", 2)
            df_func = df
        else:
            if select_cols:
                df = [df[col] for col in df if df in select_cols] if select_cols != "*" else df
            add_buffer("df = self.create_dataframe(data=" + pformat(df, compact=True, sort_dicts=False) +
                       ", force_data_types=True)\n", 2)
            df_func = df

        # Process simple arguments
        _args = []
        for v in args:
            if is_function(v):
                _args.append(v.__qualname__)
            elif isinstance(v, (BaseDataFrame,)):
                _df = "self.create_dataframe(data=" + v.export(data_types="internal") + ", force_data_types=True)"
                _args.append(_df)
            elif isinstance(v, list) and isinstance(v[0], (BaseDataFrame,)):
                _dfs = ["self.create_dataframe(data=" + dv.export(data_types="internal") + ", force_data_types=True)" for dv in v]
                _dfs = "[" + ", ".join(_dfs) + "]"
                _args.append(_dfs)
            elif isinstance(v, (str, bool, dict, list)):
                _args.append(pformat(v, compact=True, sort_dicts=False, width=800))
            else:
                _args.append(str(v))

        _args = ', '.join(_args)
        _kwargs = []

        # Process keywords arguments
        for k, v in kwargs.items():
            if is_function(v):
                _kwargs.append(k + "=" + v.__qualname__)
            elif isinstance(v, (BaseDataFrame,)):
                _df = "self.create_dataframe(data=" + v.export(data_types="internal") + ", force_data_types=True)"
                _kwargs.append(k + "=" + _df)
            elif isinstance(v, list) and isinstance(v[0], (BaseDataFrame,)):
                _dfs = ["self.create_dataframe(data=" + dv.export(data_types="internal") + ", force_data_types=True)" for dv in v]
                _dfs = "[" + ", ".join(_dfs) + "]"
                _kwargs.append(k + "=" + _dfs)
            elif isinstance(v, (str, bool, dict, list)):
                _kwargs.append(k + "=" + pformat(v, compact=True, sort_dicts=False, width=800))
            else:
                _kwargs.append(k + "=" + str(v))

        # Separator if we have positional and keyword arguments
        separator = ""
        if (not is_list_empty(args)) & (not is_list_empty(kwargs)):
            separator = ", "

        if method is None:
            add_buffer("result = df\n", 2)

        else:
            ams = ""
            for m in additional_method:
                ams += "." + m + "()"

            add_buffer("result = df." + method + "(" + _args + separator + ', '.join(
                _kwargs) + ")" + ams + "\n", 2)

        # print("expected_df", expected_df)

        failed = False

        # Apply function
        if method is None:
            expected_df = df_func
        else:
            # Here we construct the method to be applied to the source object
            _df_func = df_func
            for f in method.split("."):
                df_func = getattr(df_func, f)

            try:
                expected_df = df_func(*args, **kwargs)
            except Exception as e:
                warnings.warn(
                    f"The operation on test creation {func_test_name} failed, passing the same dataset instead")
                print(e)
                failed = True
                expected_df = _df_func
        # Additional Methods
        for m in additional_method:
            expected_df = getattr(expected_df, m)()

        # Checks if output is ok

        expected_is_df = isinstance(expected_df, (BaseDataFrame,))

        if compare_by == "df" and not expected_is_df:
            compare_by = "dict"

        if compare_by != "df" and expected_is_df:
            add_buffer("result = result.to_dict()\n", 2)

        if failed:
            add_buffer("# The following value does not represent a correct output of the operation\n", 2)
            add_buffer("expected = self.dict\n", 2)
        elif compare_by == "df":
            if expected_is_df:
                expected_df = expected_df.export(data_types="internal")
            add_buffer(f"expected = self.create_dataframe(data={expected_df}, force_data_types=True)\n", 2)
        else:
            if expected_is_df:
                expected_df = expected_df.export(data_types=False)
                add_buffer(f"expected = {expected_df}\n", 2)
            elif is_str(expected_df):
                add_buffer(f"expected = {pformat(expected_df, compact=True, sort_dicts=False)}\n", 2)
            else:
                add_buffer(f"expected = {expected_df}\n", 2)

        # Output
        if compare_by == "df":
            add_buffer("self.assertTrue(result.equals(expected, decimal=True, assertion=True))\n", 2)
        elif compare_by == "dict":
            if expected_is_df:
                add_buffer("self.assertTrue(df_dicts_equal(result, expected, assertion=True))\n", 2)
            else:
                add_buffer("self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))\n", 2)

        elif compare_by == "json":
            add_buffer("self.assertEqual(json_encoding(result), json_encoding(expected))\n", 2)
        else:
            add_buffer("self.assertEqual(result, expected)\n", 2)

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

        test_file.close()

        self.created.append((method, variant))

        # return "".join(buffer)

    def delete(self, method=None, variant=None):
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
        elif method is not None:
            variant = "_" + variant

        # Create func test name. If is None we just test the create.df function a not transform the data frame in
        # any way
        if method is None:
            method_test_name = "test_" + variant + "()"
            filename = variant + ".test"

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


default_configs = {
    "Pandas": {"engine": "pandas"},
    "Dask": {"engine": "dask", "n_partitions": 1},
    "PartitionDask": {"engine": "dask", "n_partitions": 2},
    "CUDF": {"engine": "cudf", "try_package": "cudf"},
    "DC": {"engine": "dask_cudf", "n_partitions": 1, "try_package": "dask_cudf"},
    "PartitionDC": {"engine": "dask_cudf", "n_partitions": 2, "try_package": "dask_cudf"},
    "Spark": {"engine": "spark", "try_package": "pyspark"},
    "Vaex": {"engine": "vaex", "try_package": "vaex"}
}
