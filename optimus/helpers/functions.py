
def count_items(self, col_id, col_search, new_col_feature, search_string):
    """
    This function can be used to create Spark DataFrames with frequencies for picked values of
    selected columns.

    :param col_id    column name of the columnId of dataFrame
    :param col_search     column name of the column to be split.
    :param new_col_feature        Name of the new column.
    :param search_string         string of value to be counted.

    :returns Spark Dataframe.

    Please, see documentation for more explanations about this method.

    """
    # Asserting if position is string or list:

    assert isinstance(search_string, str), "Error: search_string argument must be a string"

    # Asserting parameters are not empty strings:
    assert (
            (col_id != '') and (col_search != '') and (new_col_feature != '')), \
        "Error: Input parameters can't be empty strings"

    # Check if col_search argument is string datatype:
    self._assert_type_str(col_search, "col_search")

    # Check if new_col_feature argument is a string datatype:
    self._assert_type_str(new_col_feature, "new_col_feature")

    # Check if col_id argument is a string datatype:
    self._assert_type_str(col_id, "col_id")

    # Check if col_id to be process are in dataframe
    self._assert_cols_in_df(columns_provided=[col_id], columns_df=self._df.columns)

    # Check if col_search to be process are in dataframe
    self._assert_cols_in_df(columns_provided=[col_search], columns_df=self._df.columns)

    # subset, only PAQ and Tipo_Unidad:
    subdf = self._df.select(col_id, col_search)

    # subset de
    new_column = subdf.where(subdf[col_search] == search_string).groupBy(col_id).count()

    # Left join:
    new_column = new_column.withColumnRenamed(col_id, col_id + '_other')

    exprs = (subdf[col_id] == new_column[col_id + '_other']) & (subdf[col_search] == search_string)

    df_mod = subdf.join(new_column, exprs, 'left_outer')

    # Cleaning dataframe:
    df_mod = df_mod.drop(col_id + '_other').drop(col_search).withColumnRenamed('count', new_col_feature) \
        .dropna("any")

    print("Counting existing " + search_string + " in " + col_search)
    return df_mod.sort(col_id).drop_duplicates([col_id])