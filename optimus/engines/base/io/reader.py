class Reader:
    def __init__(self, resp, step_size, callback=None, n_rows=None, encoding="utf-8"):
        """
        Initialize a new Reader object.

        :param resp: The response object returned by the HTTP request to download the CSV file.
        :type resp: requests.Response
        :param step_size: The number of bytes to read from the response object at a time.
        :type step_size: int
        :param callback: A function to call periodically with progress updates.
        :type callback: callable
        :param n_rows: The maximum number of rows to read from the CSV file. If None, all rows are read.
        :type n_rows: int or None
        :param encoding: The character encoding to use when decoding the CSV data.
        :type encoding: str
        """
        if n_rows is None:
            self.step_size = 100000
        elif step_size is None:
            self.step_size = 100000
        elif step_size > n_rows:
            self.step_size = n_rows
        else:
            self.step_size = step_size

        self.step = step_size
        self.step_size = step_size
        self.bytes_loaded = 0
        self.total_size = int(resp.headers.get("Content-length"))
        self.callback = callback
        self.resp = resp
        self.n_rows = n_rows
        self.encoding = encoding
        self.reader = self.read_from_stream()

    def read_from_stream(self):
        """
        Read rows from the CSV file and yield them one at a time.

        :return: The next row in the CSV file, decoded using the specified encoding.
        :rtype: str
        """
        rows_read = 0
        start_row_last_step = 0
        for line in self.resp.iter_lines(chunk_size=self.step_size):
            line += b"\n"
            self.bytes_loaded += len(line)

            if self.bytes_loaded > self.step:
                if self.callback is not None:
                    self.callback(self.bytes_loaded, self.total_size, start_row_last_step, rows_read)
                self.step = self.bytes_loaded + self.step_size
                start_row_last_step = rows_read + 1
            line = line.decode(self.encoding)
            yield line

            rows_read += 1
            # print(rows_read)
            if self.n_rows is not None and rows_read >= self.n_rows:
                break

    def read(self, n=0):
        """
        Read the next row from the CSV file.

        :param n: Not used.
        :type n: int

        :return: The next row in the CSV file, decoded using the specified encoding.
        :rtype: str
        """
        try:
            return next(self.reader)
        except StopIteration:
            return ""
