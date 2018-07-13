from pyspark.sql import DataFrame
from pyspark.sql.dataframe import *

from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

# Helpers
from optimus.helpers.validators import *
from optimus.helpers.constants import *
from optimus.helpers.decorators import *
import optimus.dataframe.extension as op
import builtins


# from beartype import beartype

@add_method(DataFrame)
def rows(self):
    @add_attr(rows)
    def append(row):
        """
        Append a row at the endo of the dataframe
        :param name:
        :param value:
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

        :param self:
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
        :param parameters   List of columns in the following form: [(columnName, data_type, func),
                                                                    (columnName1, dataType1, func1)]
        :return None
        """

        assert isinstance(parameters, list), 'Error: patrameters must be a list'
        assert isinstance(parameters[0], tuple), 'Error: elements inside parameters should be a tuple'

        validate_columns_names(self, parameters, 0)

        df = self
        for column, data_type, func in parameters:

            # Checking if column has a valid datatype:
            assert (data_type in ['integer', 'float', 'string',
                                  'null']), \
                "Error: data_type only can be one of the followings options: integer, float, string, null."

            # Checking if func parameters is func data_type or None
            assert isinstance(func, (type(None),
                                     type(lambda x: x))), "Error: func argument must be a function or NoneType"

            if isfunction(func):
                func_udf = F.udf(lambda x: func(x) if check_data_type(x) == data_type else x)

            elif isinstance(func, (str, int, float)):
                assert [x[1] in TYPES[type(func)] for x in filter(lambda x: x[0] == columnName, self.dtypes)][
                    0], \
                    "Error: Column of operation and func argument must be the same global type. " \
                    "Check column type by df.printSchema()"
                func_udf = F.udf(lambda x: func if check_data_type(x) == data_type else x)

            if func is None:
                func_udf = F.udf(lambda x: None if check_data_type(x) == data_type else x)

            df = df.withColumn(column, func_udf(F.col(column).alias(column)))

        return df

    @add_attr(rows)
    def check_data_type(value):
        """
        Retunr if value is float, int or string
        :param value: value to be checked
        :return:
        """
        try:  # Try to parse (to int) register value
            int(value)
            # Add 1 if suceed:
            return 'integer'
        except ValueError:
            try:
                # Try to parse (to float) register value
                float(value)
                # Add 1 if suceed:
                return 'float'
            except ValueError:
                # Then, it is a string
                return 'string'
        except TypeError:
            return 'null'

    @add_attr(rows)
    def filter_by_type(column_name, type):
        """
        This function has built in order to deleted some type of row depending of the var type detected by python
        for Example if you have a column with
        | a |
        | 1 |
        | b |

        and you filter by type = integer the second row (1) will be eliminated
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

    @add_attr(rows)
    def lookup(columns, lookup_key=None, replace_by=None):
        """
        This method search a list of strings specified in `list_str` argument among rows
                in column dataFrame and replace them for `str_to_replace`.
        :param columns: Column name, this variable must be string dataType.
        :param lookup_key: List of strings to be replaced
        :param replace_by: string that going to replace all others present in list_str argument
        :return:
        """

        # Asserting columns is string or list:
        assert isinstance(replace_by, (str, dict)), "Error: str_to_replace argument must be a string or a dict"

        # Asserting columns is string or list:
        assert isinstance(lookup_key, list) and lookup_key != [] or (
                lookup_key is None), "Error: Column argument must be a non empty list"

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in builtins.filter(lambda t: t[1] == 'string', self.dtypes)]

        if isinstance(columns, str):
            columns = [columns]

        # Asserting if selected column datatype, lookup_key and replace_by parameters are the same:
        col_not_valids = (set(columns).difference(set([column for column in valid_cols])))
        assert (col_not_valids == set()), 'Error: The column provided is not a column string: %s' % col_not_valids

        def check(cell):
            if cell is not None and (cell in lookup_key):
                return replace_by
            else:
                return cell

        func = F.udf(lambda cell: check(cell), StringType())
        # func = pandas_udf(lambda cell: check(cell), returnType=StringType())

        # Calling udf for each row of column provided by user. The rest of dataFrame is maintained the same.
        exprs = [func(F.col(c)).alias(c) if c == columns[0] else c for c in self.columns]

        return self.select(*exprs)

    return rows
