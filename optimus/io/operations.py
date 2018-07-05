def read_csv(self, path, sep=',', header='true', infer_schema='true', *args, **kargs):
    """This funcion read a dataset from a csv file.

    :param path: Path or location of the file.
    :param sep: Usually delimiter mark are ',' or ';'.
    :param  header: Tell the function whether dataset has a header row. 'true' default.
    :param infer_schema: infers the input schema automatically from data.
    It requires one extra pass over the data. 'true' default.

    :return dataFrame
    """
    assert ((header == 'true') or (header == 'false')), "Error, header argument must be 'true' or 'false'. " \
                                                        "header must be string dataType, i.e: header='true'"
    assert isinstance(sep, str), "Error, delimiter mark argumen must be string dataType."

    assert isinstance(path, str), "Error, path argument must be string datatype."

    return self.spark.read \
        .options(header=header) \
        .options(delimiter=sep) \
        .options(inferSchema=infer_schema) \
        .csv(path, *args, **kargs)


def read_url(self, path=None, ty="csv"):
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


def json_data_loader(self, path):
    """

    :param path:
    :return:
    """
    res = open(path, 'r').read()
    print("Loading file using a pyspark dataframe for spark 2")
    data_rdd = self.__sc.parallelize([res])
    return self.spark.read.json(data_rdd)


def data_loader(self, path):
    """

    :param path:
    :return:
    """

    print("Loading file using 'SparkSession'")
    csvload = self.spark.builder.getOrCreate() \
        .read \
        .format("csv") \
        .options(header=True) \
        .options(mode="DROPMALFORMED")

    return csvload.option("inferSchema", "true").load(path)


def read_dataset_parquet(self, path):
    """This function allows user to read parquet files. It is import to clarify that this method is just based
    on the spark.read.parquet(path) Apache Spark method. Only assertion instructions has been added to
    ensure user has more hints about what happened when something goes wrong.
    :param  path    Path or location of the file. Must be string dataType.

    :return dataFrame"""
    assert isinstance(path, str), "Error: path argument must be string dataType."
    assert (("file:///" == path[0:8]) or ("hdfs:///" == path[0:8])), "Error: path must be with a 'file://' prefix \
       if the file is in the local disk or a 'path://' prefix if the file is in the Hadood file system"
    return self.spark.read.parquet(path)


def csv_to_parquet(self, input_path, output_path, sep_csv, header_csv, num_partitions=None):
    """
    This method transform a csv dataset file into a parquet.
    The method reads a existing csv file using the inputPath, sep_csv and headerCsv arguments.

    :param input_path: Address location of the csv file.
    :param output_path: Address where the new parquet file will be stored.
    :param sep_csv: Delimiter mark of the csv file, usually is ',' or ';'.
    :param header_csv: This argument specifies if csv file has header or not.
    :param num_partitions: Specifies the number of partitions the user wants to write the dataset."""

    df = self.read_csv(input_path, sep_csv, header=header_csv)

    if num_partitions is not None:
        assert (num_partitions <= df.rdd.getNumPartitions()), "Error: num_partitions specified is greater that the" \
                                                              "partitions in file store in memory."
        # Writing dataset:
        df.coalesce(num_partitions).write.parquet(output_path)

    else:
        df.write.parquet(output_path)


def write_df_as_json(self, path):
    """

    :param self:
    :param path:
    :return:
    """
    p = re.sub("}\'", "}", re.sub("\'{", "{", str(self._df.toJSON().collect())))

    with open(path, 'w') as outfile:
        # outfile.write(str(json_cols).replace("'", "\""))
        outfile.write(p)


def to_csv(self, path_name, header="true", mode="overwrite", sep=",", *args, **kargs):
    """
    Write dataframe as CSV.
    :param path_name: Path to write the DF and the name of the output CSV file.
    :param header: True or False to include header
    :param mode: Specifies the behavior of the save operation when data already exists.
                "append": Append contents of this DataFrame to existing data.
                "overwrite" (default case): Overwrite existing data.
                "ignore": Silently ignore this operation if data already exists.
                "error": Throw an exception if data already exists.
    :param sep: sets the single character as a separator for each field and value. If None is set,
    it uses the default value.
    :return: Dataframe in a CSV format in the specified path.
    """

    self._assert_type_str(path_name, "path_name")

    assert header == "true" or header == "false", "Error header must be 'true' or 'false'"

    if header == 'true':
        header = True
    else:
        header = False

    return self._df.write.options(header=header).mode(mode).csv(path_name, sep=sep, *args, **kargs)