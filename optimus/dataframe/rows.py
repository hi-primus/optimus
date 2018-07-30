from pyspark.sql import DataFrame
from pyspark.sql.dataframe import *

# from pyspark.sql import functions as F

# Helpers
import optimus.create as op
from optimus.helpers.functions import validate_columns_names, parse_columns
from optimus.helpers.constants import *
from optimus.helpers.decorators import *

from optimus.functions import filter_row_by_data_type as fbdt


@add_method(DataFrame)
def rows(self):
    @add_attr(rows)
    def append(row):
        """
        Append a row at the end of a dataframe
        :param row: List of values
        :return:
        """

        #if is_list_of_dataframes(rows):
        #    return reduce(DataFrame.union, dfs)
        df = self

        assert isinstance(row, list), "Error: row must me a list"
        assert len(row) > 0, "Error: row list must be greater that 0"
        assert len(df.dtypes) == len(row), "Error row must be the same lenght of the dataframe"

        cols = []
        values = []
        for d, r in zip(df.dtypes, row):
            col_name = d[0]
            data_type = d[1]
            if data_type in TYPES_SPARK_FUNC:
                cols.append((col_name, (TYPES_SPARK_FUNC[data_type]), True))
                values.append(r)

        values = [tuple(values)]
        new_row = op.Create.data_frame(cols, values)

        return df.union(new_row)

    @add_attr(rows)
    def filter_by_dtypes(col_name, data_type=None):
        """
        This function has built in order to filter some type of row depending of the var type detected by python
        for Example if you have a column with
        | a |
        | 1 |
        | b |

        and you filter by type = integer you will get

        | 1 |

        :param col_name:
        :param data_type:
        :return:
        """

        validate_columns_names(self, col_name)
        return self.where(fbdt(col_name, data_type))

    @add_attr(rows)
    def filter(*args, **kwargs):
        """
        Alias of Spark filter function. Return rows that match a expression
        :param args:
        :param kwargs:
        :return:
        """
        return self.filter(*args, **kwargs)

    @add_attr(rows)
    # https://chrisalbon.com/python/data_wrangling/pandas_dropping_column_and_rows/
    def drop(where=None):
        """
        Drop a file depending dataframe expression
        :param where:
        :return:
        """
        return self.where(~where)

    @add_attr(rows)
    def drop_by_dtypes(col_name, data_type=None):
        """
        Drop rows by cell data type
        :param col_name:
        :param data_type:
        :return:
        """
        validate_columns_names(self, col_name)
        return self.rows().drop(fbdt(col_name, data_type))

    @add_attr(rows)
    def drop_na(columns, how="all"):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.

        :param how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its
        values are null. The default is 'all'.
        :return: Returns a new DataFrame omitting rows with null values.
        """

        assert isinstance(how, str), "Error, how argument provided must be a string."

        assert how == 'all' or (
                how == 'any'), "Error, how only can be 'all' or 'any'."

        columns = parse_columns(self, columns)

        return self.dropna(how, subset=columns)

    @add_attr(rows)
    def drop_duplicates(columns=None):
        """
        Drop duplicates values in a dataframe
        :param columns: List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """

        columns = parse_columns(self, columns)
        return self.drop_duplicates(subset=columns)

    @add_attr(rows)
    def drop_first():
        """
        Remove first row in a dataframe
        :return:
        """
        return self.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

    return rows
