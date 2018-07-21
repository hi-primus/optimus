from pyspark.sql import DataFrame
from pyspark.sql.dataframe import *

from pyspark.sql import functions as F

# Helpers
from optimus.helpers.functions import *
from optimus.helpers.constants import *
from optimus.helpers.decorators import *
import optimus.create as op

import builtins


@add_method(DataFrame)
def rows(self):
    @add_attr(rows)
    def append(row):
        """
        Append a row at the end of a dataframe
        :param row: List of values
        :return:
        """
        df = self

        assert isinstance(row, list), "Error: row must me a list"
        assert len(row) > 0, "Error: row list must be greater that 0"

        assert len(df.dtypes) == len(row), "Error row must be the same lenght of the dataframe"

        cols = []
        values = []
        for d, r in zip(df.dtypes, row):
            col_name = d[0]
            data_type = d[1]
            if data_type in DICT_TYPES:
                cols.append((col_name, (DICT_TYPES[data_type]), True))
                values.append(r)

        values = [tuple(values)]
        new_row = op.Create.data_frame(cols, values)

        return df.union(new_row)

    @add_attr(rows)
    def replace(search, change_to, columns):
        """

        :param search:
        :param change_to:
        :param columns:
        :return:
        """

        columns = self._parse_columns(columns)
        return self.replace(search, change_to, subset=columns)

    @add_attr(rows)
    def apply_by_type(parameters):
        """
        This functions makes the operation in column elements that are recognized as the same type that the data_type
        argument provided in the input function.

        Columns provided in list of tuples cannot be repeated
        :param parameters: List of columns in the following form: [(columnName, data_type, func),
                                                                    (columnName1, dataType1, func1)]
        :return None
        """

        assert isinstance(parameters, list), 'Error: patrameters must be a list'
        assert isinstance(parameters[0], tuple), 'Error: elements inside parameters should be a tuple'

        validate_columns_names(self, parameters, 0)

        df = self
        for column, data_type, var_or_func in parameters:

            # Checking if column has a valid datatype:
            assert (data_type in ['integer', 'float', 'string',
                                  'null']), \
                "Error: data_type only can be one of the followings options: integer, float, string, null."

            if isfunction(var_or_func):
                def _apply_by_type(x):
                    return var_or_func(x) if check_data_type(x) == data_type else x

            else:

                def _apply_by_type(x):
                    return var_or_func if check_data_type(x) == data_type else x

            func_udf = F.udf(_apply_by_type)

            df = df.withColumn(column, func_udf(F.col(column).alias(column)))

        return df

    @add_attr(rows)
    def filter_by_type(column_name, type=None):
        """
        This function has built in order to deleted some type of row depending of the var type detected by python
        for Example if you have a column with
        | a |
        | 1 |
        | b |

        and you filter by type = integer the second row (1) will be eliminated
        :param column_name:
        :param type:
        :return:
        """

        validate_columns_names(self, column_name)

        # Asserting if dataType argument has a valid type:
        assert (type in ['integer', 'float', 'string',
                         'null']), \
            "Error: type only can be one of the followings options: integer, float, string, null."

        func = F.udf(check_data_type, StringType())

        temp_col_name = "type_optimus"

        return self.withColumn(
            temp_col_name,
            func(F.col(column_name))) \
            .where((F.col(temp_col_name) != type)).drop(temp_col_name)  # delete rows not matching the type

    return rows
