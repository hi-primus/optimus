import logging

from optimus.helpers.checkit import is_str, is_list_empty, is_list, is_numeric, is_list_of_numeric, is_list_of_strings, \
    is_list_of_tuples


class Test:
    def __init__(self, op=None, df=None, name=None, imports=None):
        """
        Create python code with unit test functions for Optimus.
        :param df: Spark Dataframe
        :param name: Name of the Test Class
        :param imports: Libraries to be added

        """
        self.op = op
        self.df = df
        self.name = name
        self.imports = imports

    def run(self, *args):

        """
        Return the tests in text format
        :param args: list of create functions
        :return:
        """

        filename = "test_" + self.name + ".py"
        test_file = open(filename, 'w', encoding='utf-8')

        _imports = [
            "from pyspark.sql.types import *",
            "from optimus import Optimus"
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
        for t in args:
            test_file.write(t)

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
        else:
            func_test_name = "test_" + func.replace(".", "_") + suffix + "()"

        print("Creating {test} test function...".format(test=func_test_name))
        logging.info(func_test_name)

        add_buffer("@staticmethod\n")
        add_buffer("def " + func_test_name + ":\n")

        if df is not None:
            source_df = "\tsource_df=op.create.df(" + df.export() + ")\n"
            df_func = df
            add_buffer(source_df)
        else:
            df_func = self.df

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
            add_buffer("\tactual_df = source_df." + func + "(" + _args + separator + ','.join(_kwargs) + ")\n")

        # Apply function to the dataframe
        if func is None:
            df_result = self.op.create.df(*args, **kwargs)
        else:
            for f in func.split("."):
                df_func = getattr(df_func, f)

            df_result = df_func(*args, **kwargs)

        if output == "df":
            expected = "\texpected_df = op.create.df(" + df_result.export() + ")\n"
        elif output == "json":
            if is_str(df_result):
                df_result = "'" + df_result + "'"
            else:
                df_result = str(df_result)
            expected = "\texpected_value =" + df_result + "\n"

        add_buffer(expected)

        if output == "df":
            add_buffer("\tassert (expected_df.collect() == actual_df.collect())\n")
        elif output == "json":
            add_buffer("\tassert (expected_value == actual_df)\n")

        return "".join(buffer)
