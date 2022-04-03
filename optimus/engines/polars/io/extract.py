import tarfile

import glob

from optimus.helpers.functions import prepare_path


class Extract:
    @staticmethod
    def gz(path, *args, **kwargs):
        """
        Loads a dataframe from a gz file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the internal function
        :param kwargs: custom keyword arguments to be passed to the internal function
        :return: Spark Dataframe
        """
        file, file_name = prepare_path(path, "gz")

        import gzip
        import shutil
        with gzip.open(file, 'rb') as f_in:
            print(f_in)
            with open('file.txt', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        print(file, file_name)

    @staticmethod
    def tar(input_file, output_path="", *args, **kwargs):
        """
        Extract .tar or .tar.gz files
        :param input_file:
        :param output_path:
        :param args:
        :param kwargs:
        :return: List of file names
        """

        if input_file.endswith("tar.gz"):
            mode = "r:gz"
        elif input_file.endswith("tar"):
            mode = "r:"

        tar = tarfile.open(input_file, mode)
        tar.extractall(path=output_path)
        tar.close()

        # filename
        output_path = input_file.split('.')[0]
        return glob.glob(output_path)
