class DataFrameBaseRows():

    def _sort(self, dfd, col_name, ascending):
        return dfd.sort_values(col_name, ascending=ascending)
