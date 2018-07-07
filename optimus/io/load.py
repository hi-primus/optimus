class Load:

    def url(self, path=None, ty="csv"):
        """
        Reads dataset from URL.
        :param path: string for URL to read
        :param ty: type of the URL backend (can be csv or json)
        :return: pyspark dataframe from URL.
        """
        if "https://" in str(path) or "http://" in str(path) or "file://" in str(path):
            if ty is 'json':
                self.url = str(path)
                return self.json_load_spark_data_frame_from_url(str(path))
            else:
                return self.load_spark_data_frame_from_url(str(path))
        else:
            print("Unknown sample data identifier. Please choose an id from the list below")