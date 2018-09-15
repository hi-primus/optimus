from optimus.helpers.checkit import is_str, is_list_empty


class Test:
    def __init__(self):
        self.buffer = []

        pass

    def add(self, value):
        self.buffer.append(value)

    def run(self, *args):
        """
        Print all the results
        :param args:
        :return:
        """
        self.add("from pyspark.sql.types import *\n")
        self.add("from optimus import Optimus\n")

        for a in args:
            print(a)

    def create(self, df, func, output, *args, **kwargs):
        """
        This is a helper function that output python tests for Optimus transformations.
        :param df: Spark Dataframe
        :param func: Spark dataframe function to be tested
        :param output: can be a 'df' or a 'json'
        :param args: Arguments to be used in the function
        :param kwargs: Keyword arguments to be used in the functions
        :return:
        """

        func_test_name = "test_" + func.replace(".", "_") + "()"

        self.add("@staticmethod")
        self.add("def " + func_test_name + ":\n")

        source_df = "\tsource_df=op.create.df(" + df.export() + ")\n"

        self.add(source_df)

        # Process simple arguments
        _args = []
        for v in args:
            if is_str(v):
                _args.append("'" + v + "'")

        _args = ','.join(_args)

        _kwargs = []

        # Process keywords arguments
        for k, v in kwargs.items():
            _kwargs.append(k + "=" + str(v))

        separator = ""
        if (not is_list_empty(args)) & (not is_list_empty(kwargs)):
            separator = ","

        self.add("\tactual_df = source_df." + func + "(" + _args + separator + ','.join(_kwargs) + ")\n")

        # Process functions
        method_to_call = df
        for f in func.split("."):
            method_to_call = getattr(method_to_call, f)

        df_result = method_to_call(*args, **kwargs)

        if output == "df":
            expected = "\texpected_df = op.create.df(" + df_result.export() + ")\n"
        elif output == "json":
            if is_str(df_result):
                df_result = "'" + df_result + "'"
            else:
                df_result = str(df_result)
            expected = "\texpected_json =" + df_result + "\n"

        self.add(expected)

        if output == "df":
            self.add("\tassert (expected_df.collect() == actual_df.collect())\n")
        elif output == "json":
            self.add("\tassert (expected_json == actual_df)\n")

        self.add(func_test_name + "\n")

        if False:
            print("-----------")
            print("Original dataframe")

            df.table()

            print("Please check that expression bellow is what is expected")

            if output == "df":
                df_result.table()
            elif output == "json":
                print(df_result)
