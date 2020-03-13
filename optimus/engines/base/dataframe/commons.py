# Columns
from optimus.helpers.columns import parse_columns, get_output_cols


# CUDF not support regex right now so we have to convert the cudf to pandas and then back.
def extract(df, input_cols, output_cols, regex):
    input_cols = parse_columns(df, input_cols)
    output_cols = get_output_cols(input_cols, output_cols)

    for input_col, output_col in zip(input_cols, output_cols):
        df[output_col] = df[input_col].str.extract(regex)

    return df
