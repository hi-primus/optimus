from optimus.helpers.decorators import add_method
from pyspark.sql import DataFrame
from optimus.spark import get_spark

class Columns:
    def __init__(self):
        spark = get_spark()
        self.sc = spark.sparkContext

    def create(self, name=None, value=None):
        """
        Create a column
        """
        self.sc.withColumn(name, value)

        print("Column created")

    def rename(self, columns_pair):
        """"
        This functions change the name of a column(s) dataFrame.
        :param columns_pair: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        """
        # Check that the 1st element in the tuple is a valis set of columns
        columns = self._parse_columns(columns_pair, 0)

        # Rename cols
        columns = [col(column[0]).alias(column[1]) for column in columns_pair]

        return self.select(columns)

    def select(self, by_index=None, by_name=None, regex=None):
        if by_index:
            print("Select column by index")
        elif by_name:
            print("Select column by column name")
        elif regex:
            print("Select column by regex")

    #@dispatch(int, int)
    #def move(self, from_column, to_column):
    #    print("Column Moved. int params")

    #@dispatch(str, str)
    #def move(self, from_column, to_column):
    #    print("Column Moved. str params")

    def drop(self, by_index=None, by_name=None, regex=None):
        print("Drop column")


@add_method(DataFrame)
def columns(self):
    if not hasattr(self, 'columns_'):
        self.columns_ = Columns()
    return self.columns_
