from optimus.helpers.checkit import is_str, is_list_empty


class Test:
    def __init__(self, df=None, name=None, imports=None):
        """
        Create python code with unit test for Optimus.
        :param df: Spark Dataframe
        :param name: Name of the Test Class
        :param imports: Libraries to be added

        """

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
        test_file = open(filename, 'w')

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

        test_file.write("op = Optimus()\n")

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

        buffer = []

        def add_buffer(value):
            buffer.append("\t" + value)

        func_test_name = "test_" + func.replace(".", "_") + "()"

        add_buffer("@staticmethod\n")
        add_buffer("def " + func_test_name + ":\n")

        if df is not None:
            source_df = "\tsource_df=op.create.df(" + df.export() + ")\n"
            method_to_call = df
            add_buffer(source_df)
        else:
            method_to_call = self.df

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

        add_buffer("\tactual_df = source_df." + func + "(" + _args + separator + ','.join(_kwargs) + ")\n")

        # Process functions

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
            expected = "\texpected_value =" + df_result + "\n"

        add_buffer(expected)

        if output == "df":
            add_buffer("\tassert (expected_df.collect() == actual_df.collect())\n")
        elif output == "json":
            add_buffer("\tassert (expected_value == actual_df)\n")

        # add_buffer(func_test_name + "\n")

        if False:
            print("-----------")
            print("Original dataframe")

            df.table()

            print("Please check that expression bellow is what is expected")

            if output == "df":
                df_result.table()
            elif output == "json":
                print(df_result)
        return "".join(buffer)
