import pandavro as pdx
import os
from optimus.helpers.logger import logger


class Save:
    def __init__(self, root):
        self.root = root

    def json(self, path, orient="records", *args, **kwargs):
        """
        Save data frame in a json file
        :param path: path where the spark will be saved.
        :param orient:

        :return:
        """
        df = self.root.data

        try:
            df.to_json(path, orient=orient, *args, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    def csv(self, path, mode="w",show_path=False, **kwargs):
        """
        Save data frame to a CSV file.
        :param path: path where the spark will be saved.
        :param mode: 'rb', 'wt', etc
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        try:
            df = self.root.data
            # columns = parse_columns(self, "*",
            #                         filter_by_column_dtypes=["date", "array", "vector", "binary", "null"])
            # df = df.cols.cast(columns, "str").repartition(num_partitions)

            # Dask reference
            # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
            # df.to_csv(filename=path)

            if show_path is True:
                print(os.path.abspath(path))

            df.to_csv(path, index=False, mode=mode)

        except IOError as error:
            logger.print(error)
            raise

    def excel(self, path, **kwargs):
        """
        Save data frame to a CSV file.
        :param path: path where the spark will be saved.
        :param mode: 'rb', 'wt', etc
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        try:
            df = self.root.data
            df.to_excel(path, index=False)

        except IOError as error:
            logger.print(error)
            raise

    def xml(self, filename=None, mode='w'):
        df = self.root.data

        def row_to_xml(row):
            xml = ['<item>']
            for i, col_name in enumerate(row.index):
                xml.append('  <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
            xml.append('</item>')
            return '\n'.join(xml)

        res = '\n'.join(df.apply(row_to_xml, axis=1))

        if filename is None:
            return res
        with open(filename, mode) as f:
            f.write(res)

    def parquet(self, path, *args, **kwargs):
        """
        Save data frame to a parquet file
        :param path: path where the spark will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """

        # This character are invalid as column names by parquet
        invalid_character = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = self.root.cols.rename(func)

        try:
            df.data.to_parquet(path)
        except IOError as e:
            logger.print(e)
            raise

    def avro(self, path):
        pdx.to_avro(path, self.root.data)
