from pyspark.sql import DataFrame
from pyspark.sql.dataframe import *

from pyspark.sql import functions as F

# Helpers
from optimus.helpers.functions import *
from optimus.helpers.constants import *
from optimus.helpers.decorators import *
import optimus.create as op
from optimus.functions import filter_by_data_type as fbdt

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
            if data_type in TYPES_SPARK_FUNC:
                cols.append((col_name, (TYPES_SPARK_FUNC[data_type]), True))
                values.append(r)

        values = [tuple(values)]
        new_row = op.Create.data_frame(cols, values)

        return df.union(new_row)


    @add_attr(rows)
    def filter_by_dtype(col_name, data_type=None):
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

        # Asserting if dataType argument has a valid type:
        assert (data_type in TYPES_PROFILER), \
            "Error: type only can be one of the followings options: integer, float, string, null."

        return self.where(fbdt(col_name, data_type))

    @add_attr(rows)
    def drop_by_dtype(columns, data_type=None):
        """

        :param columns:
        :param data_type:
        :return:
        """

        # Asserting if dataType argument has a valid type:
        assert (data_type in TYPES_PROFILER), \
            "Error: type only can be one of the followings options: integer, float, string, null."

        columns = parse_columns(self, columns)

        df = self
        for c in columns:
            df.where(~fbdt(c, data_type))
        return df

    return rows
