from pyspark.sql.session import SparkSession
from multipledispatch import dispatch
from optimus.dataframe.extension import Create

class Optimus:
    def __init__(self, spark=None):
        self.spark = spark

        if self.spark is None:
            print("Creating Spark Session...")
            self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        else:
            print("Using a created Spark Session...")

        self.sc = self.spark.sparkContext
        self.rows = self.Rows()
        self.columns = self.Columns()
        self.create = Create(self.spark)
        self.load = self.Load()

        print("Done.")

    def cast(self):
        print("Cast")

    class Load:
        def csv(self):
            print("CSV file Loaded")

        def parquet(self):
            print()

    class Save:
        def csv(self):
            print("CSV file saved")

    class Rows:
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

    class Columns:
        def create(self, name=None, value=None):
            """
            Create a column
            """
            # train.withColumn('Purchase_new', train.Purchase /2.0).select('Purchase','Purchase_new').show(5)
            print("Column created")

        def select(self, by_index=None, by_name=None, regex=None):
            if by_index:
                print("Select column by index")
            elif by_name:
                print("Select column by column name")
            elif regex:
                print("Select column by regex")

        @dispatch(int, int)
        def move(self, from_column, to_column):
            print("Column Moved. int params")

        @dispatch(str, str)
        def move(self, from_column, to_column):
            print("Column Moved. str params")

        def drop(self, by_index=None, by_name=None, regex=None):
            print("drop")

        def rename(self, names):
            print("rename")
