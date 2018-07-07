from optimus.helpers.decorators import add_method
from pyspark.sql import DataFrame
from optimus.spark import get_spark

class Rows:
    def __init__(self):
        spark = get_spark()
        self.sc = spark.sparkContext
        
    def create(self):
        print("Row Created")

    def select(self, index=None, exp=None):
        """
        You can select row by index or by expression. The expression can str = "house"
        """

        # df.filter(df['age'] > 21).show()

        if index:
            print("This is the content of a Row selected by index")
        elif exp:
            print("This is the content of a Row selected by regex")

    def update(self):
        print("Row updated with new content")

    def delete(self):
        print("Row Deleted")

    def apply(self):
        print("Row Apply")

@add_method(DataFrame)
def rows(self):
    if not hasattr(self, 'columns_'):
        self.rows_ = Rows()
    return self.rows_