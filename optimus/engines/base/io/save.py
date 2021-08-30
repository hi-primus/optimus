import warnings

from optimus.helpers.types import *

DEFAULT_MODE = "w"
DEFAULT_NUM_PARTITIONS = 1


class BaseSave:
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def file(self, path: str, *args, **kwargs):
        """

        :param path:
        :param args:
        :param kwargs:
        :return:
        """
        if "." not in path:
            warnings.warn("No file extension found in path, saving to Parquet file.")
            file_ext = "parquet"
        else:
            file_ext = path.split(".")[-1]

        funcs = {
            'xls': 'excel',
            'xlsx': 'excel'
        }

        func_name = funcs.get(file_ext, file_ext.lower())

        func = getattr(self, func_name, None)

        if not callable(func):
            raise ValueError(f"No function found for extension '{file_ext}'")

        return func(path, *args, **kwargs)

    def csv(self, path, *args, **kwargs):
        """
        Save data frame to a CSV file.
        :param path: path where the spark will be saved.
        :param mode: 'rb', 'wt', etc
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        raise NotImplementedError("Not implemented yet")

    def xml(self, filename: str = None, mode: str = 'w'):
        """

        :param filename:
        :param mode:
        :return:
        """
        dfd = self.root.data

        columns = list(dfd.columns)

        xml_data = ['<root>']
        for i in range(len(dfd)):
            xml_data.append('<row>')  # Opening element tag
            for column in columns:
                # writing sub-elements
                xml_data.append(f'    <{column}>{dfd[column][i]}</{column}>')
            xml_data.append('</row>')  # Closing element tag
        xml_data.append('</root>')

        if filename is None:
            return '\n'.join(xml_data)

        folder_name = "/".join(filename.split('/')[0:-1])

        from pathlib import Path
        Path(folder_name).mkdir(parents=True, exist_ok=True)

        with open(filename, mode, encoding="utf-8") as f:
            f.write('\n'.join(xml_data))

    def json(self, path, *args, **kwargs):
        """
        Save data frame in a json file
        :param path: path where the spark will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                "append": Append contents of this DataFrame to existing data.
                "overwrite" (default case): Overwrite existing data.
                "ignore": Silently ignore this operation if data already exists.
                "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """
        raise NotImplementedError("Not implemented yet")

    def excel(self, path, mode=DEFAULT_MODE, *args, **kwargs):
        """
         Save data frame to a CSV file.
         :param path: File path or object
         :param mode: Python write mode, default ‘w’.
         it uses the default value.
         :return: Dataframe in a CSV format in the specified path.
         """
        raise NotImplementedError("Not implemented yet")

    def avro(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def parquet(self, path, mode=DEFAULT_MODE, num_partitions=DEFAULT_NUM_PARTITIONS, *args, **kwargs):
        """
        Save data frame to a parquet file
        :param path: File path or object
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """
        raise NotImplementedError("Not implemented yet")

    def orc(self, path, *args, **kwargs):
        """

        :param path:
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Not implemented yet")

    def hdf5(self, path, *args, **kwargs):
        """

        :param path:
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Not implemented yet")

    def database_table(self, table, db, *args, **kwargs):
        """

        :param table:
        :param db:
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Not implemented yet")
