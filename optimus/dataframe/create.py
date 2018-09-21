from ast import literal_eval
from fastnumbers import isint, isfloat

import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

# Helpers
from optimus.helpers.checkit import is_tuple, is_, is_one_element, is_list_of_tuples, is_date, is_binary, is_datetime, \
    is_list, is_str, is_float, is_int, is_bool, str_to_array, str_to_boolean, str_to_date, is_list_of_str
from optimus.helpers.functions import get_spark_dtypes_object, parse_spark_dtypes
from optimus.spark import Spark


class Create:

    @staticmethod
    def infer(value):

        result = None
        # print(v)
        if value is None:
            result = "null"
        elif is_bool(value):
            result = "bool"
        elif isint(value):
            result = "int"

        elif isfloat(value):
            result = "float"

        elif is_list(value):
            result = ArrayType(Create.infer(value[0]))

        elif is_datetime(value):
            result = "datetime"

        elif is_date(value):
            result = "date"

        elif is_binary(value):
            result = "binary"

        elif is_str(value):
            if str_to_boolean(value):

                result = "bool"
            elif str_to_date(value):
                result = "string"  # date
            elif str_to_array(value):
                result = "string"  # array
            else:
                result = "string"

        return get_spark_dtypes_object(result)

    @staticmethod
    def infer1(value):

        result = None
        # print(v)
        if value is None:
            result = "null"
        elif is_bool(value):
            result = "bool"
        elif isint(value):
            result = "int"

        elif isfloat(value):
            result = "float"

        elif is_list(value):
            result = "string"

        elif is_datetime(value):
            result = "datetime"

        elif is_date(value):
            result = "date"

        elif is_binary(value):
            result = "binary"

        elif is_str(value):
            if str_to_boolean(value):

                result = "bool"
            elif str_to_date(value):
                result = "string"
            elif str_to_array(value):
                result = "string"
            else:
                result = "string"

        return get_spark_dtypes_object(result)

    @staticmethod
    def data_frame(cols=None, rows=None, pdf=None):
        """
        Helper to create a Spark dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples if vals with the same number and types that cols
        :param pdf: a pandas dataframe
        :return: Dataframe
        """
        if is_(pdf, pd.DataFrame):
            result = Spark.instance.spark.createDataFrame(pdf)
        else:

            specs = []
            # Process the rows
            if not is_list_of_tuples(rows):
                rows = [(i,) for i in rows]

            # Process the columns
            for c, r in zip(cols, rows[0]):
                # Get columns name

                if is_one_element(c):
                    col_name = c

                    var_type = Create.infer(r)
                    nullable = True

                elif is_tuple(c):

                    # Get columns data type
                    col_name = c[0]
                    var_type = get_spark_dtypes_object(c[1])

                    count = len(c)
                    if count == 2:
                        nullable = True
                    elif count == 3:
                        nullable = c[2]

                # If tuple has not the third param with put it to true to accepts Null in columns
                specs.append([col_name, var_type, nullable])
            print(specs)
            struct_fields = list(map(lambda x: StructField(*x), specs))

            result = Spark.instance.spark.createDataFrame(rows, StructType(struct_fields))

        return result

    df = data_frame
