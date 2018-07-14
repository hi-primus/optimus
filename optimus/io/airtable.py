import re


class Airtable:
    def __init__(self, path):
        # Setting airtable dataset variable
        self._air_table = None
        # Setting spark as a global variable of the class
        self.spark = SparkSession()
        self.sc = self.spark.sparkContext
        # Reading dataset
        self.read_air_table_csv(path)

    def read_air_table_csv(self, path):
        """This function reads the airtable or csv file that user have to fill when the original dataframe
        is analyzed.

        The path could be a download link or a path from a local directory.
        When a local directory is provided, it is necessary to write the corresponding prefix. i.e.:
        file://.... or hdfs://...
        """

        if re.match("https", path):
            print("Reading file from web...")
            # Doing a request to url:
            response = urllib.request.urlopen(path)
            # Read obtained data:
            data = response.read()  # a `bytes` object
            # Decoding data:
            text = data.decode('utf-8')
            # Here text is splitted by ',' adn replace
            values = [line.replace("\"", "").split(",") for line in text.split("\n")]

            # Airtable datafra that corresponds to airtable table:
            self._air_table = self.sc.parallelize(values[1:]).toDF(values[0])
            print("Done")

        else:
            print("Reading local file...")
            self._air_table = self.spark.read \
                .format('csv') \
                .options(header='true') \
                .options(delimiter=",") \
                .options(inferSchema='true') \
                .load(path)
            print("Done")

    def set_air_table_df(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, pyspark.sql.data_frame.DataFrame), "Error: df argument must a sql.dataframe type"
        self._air_table = df

    def get_air_table_df(self):
        """This function return the dataframe of the class"""
        return self._air_table

    @classmethod
    def extract_col_from_df(cls, df, column):
        """This function extract column from dataFrame.
        Returns a list of elements of column specified.

        :param df   Input dataframe
        :param column Column to extract from dataframe provided.
        :return list of elements of column extracted
        """
        return [x[0] for x in df.select(column).collect()]

    def read_col_names(self):
        """This function extract the column names from airtable dataset.

        :return list of tuples: [(oldNameCol1, newNameCol1),
                (oldNameCol2, newNameCol2)...]

                Where oldNameColX are the original names of columns in
                dataset, names are extracted from the airtable
                dataframe column 'Feature Name'.

                On the other hand, newNameColX are the new names of columns
                of dataset. New names are extracted from the airtable
                dataframe  'Code Name'
                                 """

        return list(zip(self.extract_col_from_df(self._air_table, 'Feature Name'),
                        self.extract_col_from_df(self._air_table, 'Code Name')))

    def read_new_col_names(self):
        """This function extract the new column names from airtable dataset.

        :return list of new column names. Those names are from column 'Code Name'
                                 """

        return self.extract_col_from_df(self._air_table, 'Code Name')

    def read_old_col_names(self):
        """This function extract the old column names from airtable dataset.

        :return list of old column names. Those names are from column 'Feature Name'
        """
        return self.extract_col_from_df(self._air_table, 'Feature Name')

    def read_col_types(self, names='newNames'):
        """This function extract the types of columns detailled in airtable dataset.

        :param  names   Name columns of dataFrame.  names variable consist of a list
                of columns read it from airtable dataFrame in the column specified.
                 names as argument can only have two values as input of this function:
                 'newNames' and 'oldNames. When 'newNames' is provided, the names are
                 read from 'Code Name' airtable dataFrame. On the other hand, when
                 'oldNames' is provided, 'Feature Name' is read it from airtable
                 dataFrame.


                 Column names are read using the function readNewColNames(self).
                 Types are extracted from 'DataType'

        :return list of colNames and their data types in the following form:
                [(colName1, 'string'), (colName2, 'integer'), (colName3, 'float')]
        """

        assert (names == 'newNames') or (names == 'oldNames'), "Error, names argument" \
                                                               "only can be the following values:" \
                                                               "'newNames' or 'oldNames'"

        col_names = {'newNames': 'Code Name', 'oldNames': 'Feature Name'}
        return list(zip(self.extract_col_from_df(self._air_table, col_names[names]),
                        self.extract_col_from_df(self._air_table, 'DataType')))
