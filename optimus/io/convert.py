class Convert:
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