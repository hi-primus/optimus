import errno
import os
from io import UnsupportedOperation

import pyspark

from optimus.helpers.checkit import is_str, is_list_empty, is_list, is_numeric, is_list_of_numeric, is_list_of_strings, \
    is_list_of_tuples, is_function
from optimus.helpers.logger import logger


class Test:
    def __init__(self, op=None, df=None, name=None, imports=None, path=None, final_path=None, source="source_df"):
        """
        Create python code with unit test functions for Optimus.
        :param op: optimus instance
        :param df: Spark Dataframe
        :param name: Name of the Test Class
        :param imports: Libraries to be added
        :type path: folder where the tes will be written

        """
        self.op = op
        self.df = df
        self.name = name
        self.imports = imports
        self.path = path
        self.final_path = final_path

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
        _imports = [
            "from pyspark.sql.types import *",
            "from optimus import Optimus",
            "from optimus.helpers.functions import json_enconding "
        ]
        if self.imports is not None:
            for i in self.imports:
                _imports.append(i)

        # Imports
        for i in _imports:
            test_file.write(i + "\n")

        test_file.write("op = Optimus(master='local')\n")

        # Global Dataframe
        if self.df is not None:
            source_df = "source_df=op.create.df(" + self.df.export() + ")\n"
            test_file.write(source_df)

        # Class name
        cls = "class Test" + self.name + "(object):\n"

        test_file.write(cls)

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

        test_file.close()
        print("Done")

    def create(self, df, func, suffix=None, output="df", *args, **kwargs):
        """
        This is a helper function that output python tests for Spark Dataframes.
        :param df: Spark Dataframe
        :param suffix: The create method will try to create a test function with the func param given.
        If you want to test a function with different params you can use suffix.
        :param func: Spark dataframe function to be tested
        :param output: can be a 'df' or a 'json'
        :param args: Arguments to be used in the function
        :param kwargs: Keyword arguments to be used in the functions
        :return:
        """

        buffer = []

        def add_buffer(value):
            buffer.append("\t" + value)

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

        print("Creating {test} test function...".format(test=func_test_name))
        logger.print(func_test_name)

        add_buffer("@staticmethod\n")
        add_buffer("def " + func_test_name + ":\n")

        source = "source_df"
        if df is None:
            # Use the main df
            df_func = self.df
        elif isinstance(df, pyspark.sql.dataframe.DataFrame):
            source_df = "\tsource_df=op.create.df(" + df.export() + ")\n"
            df_func = df
            add_buffer(source_df)
        else:
            # TODO: op is not supposed to be hardcoded
            source = "op"
            df_func = df

        # Process simple arguments
        _args = []
        for v in args:
            if is_str(v):
                _args.append("'" + v + "'")
            elif is_numeric(v):
                _args.append(str(v))
            elif is_list(v):
                if is_list_of_strings(v):
                    lst = ["'" + x + "'" for x in v]
                elif is_list_of_numeric(v):
                    lst = [str(x) for x in v]
                elif is_list_of_tuples(v):
                    lst = [str(x) for x in v]

                _args.append('[' + ','.join(lst) + ']')
            elif is_function(v):
                _args.append(v.__qualname__)
            # else:
            #     import marshal
            #     code_string = marshal.dumps(v.__code__)
            #     add_buffer("\tfunction = '" + code_string + "'\n")
            # import marshal, types
            #
            # code = marshal.loads(code_string)
            # func = types.FunctionType(code, globals(), "some_func_name")

        _args = ','.join(_args)
        _kwargs = []

        # print(_args)
        # Process keywords arguments
        for k, v in kwargs.items():
            if is_str(v):
                v = "'" + v + "'"
            _kwargs.append(k + "=" + str(v))

        # Separator if we have positional and keyword arguments
        separator = ""
        if (not is_list_empty(args)) & (not is_list_empty(kwargs)):
            separator = ","

        if func is None:
            add_buffer("\tactual_df = source_df\n")
        else:
            add_buffer("\tactual_df =" + source + "." + func + "(" + _args + separator + ','.join(_kwargs) + ")\n")

        # Apply function to the dataframe
        if func is None:
            df_result = self.op.create.df(*args, **kwargs)
        else:
            # Here we construct the method to be applied to the source object
            for f in func.split("."):
                df_func = getattr(df_func, f)

            df_result = df_func(*args, **kwargs)

        if output == "df":
            df_result.table()
            expected = "\texpected_df = op.create.df(" + df_result.export() + ")\n"
        elif output == "json":
            print(df_result)
            if is_str(df_result):
                df_result = "'" + df_result + "'"
            else:
                df_result = str(df_result)
            add_buffer("\tactual_df =json_enconding(actual_df)\n")

            expected = "\texpected_value =json_enconding(" + df_result + ")\n"
        else:
            expected = "\t\n"

        add_buffer(expected)

        if output == "df":
            add_buffer("\tassert (expected_df.collect() == actual_df.collect())\n")
        elif output == "json":
            add_buffer("\tassert (expected_value == actual_df)\n")

        filename = self.path + "//" + filename
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        # write file
        test_file = open(filename, 'w', encoding='utf-8')

        for b in buffer:
            test_file.write(b)

        # return "".join(buffer)

    def delete(self, df, func, suffix=None, output="df", *args, **kwargs):
        """
        This is a helper function that output python tests for Spark Dataframes.
        :param df: Spark Dataframe
        :param suffix: The create method will try to create a test function with the func param given.
        If you want to test a function with different params you can use suffix.
        :param func: Spark dataframe function to be tested
        :param output: can be a 'df' or a 'json'
        :param args: Arguments to be used in the function
        :param kwargs: Keyword arguments to be used in the functions
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
        except FileNotFoundError:
            print("File NOT found")
