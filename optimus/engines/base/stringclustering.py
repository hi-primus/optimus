import copy
from optimus.engines.base.ml.contants import FINGERPRINT_COL
from optimus.helpers.columns import parse_columns, name_col


class Clusters:
    type = None
    clusters = {}
    
    
    def __init__(self, clusters):
        self.clusters = clusters


    def __repr__(self):
        return str(self.to_dict())
        
    
    def set_suggestion(self, suggestion_or_id, new_value, column = 0):

        if isinstance(suggestion_or_id, (str,)):
            for i in range(len(self.clusters[column])):
                if (self.clusters[column][i]["suggestion"] == suggestion_or_id):
                    suggestion_or_id = i
                    break
                    
        self.clusters[column][suggestion_or_id]["suggestion"] = new_value
        
    
    def to_dict(self, columns="*", limit_clusters = None, limit_suggestions = None, verbose=False):
        result = {}
        
        columns = self._parse_columns(columns)
        
        
        for column in columns:
            result[column] = {}
            for cluster in self.clusters[column][0:limit_clusters]:
                if verbose:
                    cluster_name = cluster["suggestion"]
                    _cluster = copy.deepcopy(cluster)
                    del _cluster["suggestion"]
                    result[column][cluster_name] = _cluster
                else:
                    result[column][cluster["suggestion"]] = cluster["suggestions"][0:limit_suggestions]
        
        return result
    
    
    def _parse_columns(self, columns):
        
        if columns == "*":
            columns = list(self.clusters.keys())
        else:
            for column in columns:
                if not self.clusters.get(column, False) and isinstance(column, (int,)):
                    column = list(self.clusters.keys())[column]
                    if not self.clusters.get(column, False):
                        raise
        
        return columns
    
    
    def display(self, columns="*", limit_clusters = None, limit_suggestions = None, verbose=True):
        return self.to_dict(columns, limit_clusters, limit_suggestions, verbose)

def fingerprint(df, input_cols):
    """
    Create the fingerprint for a column
    :param df: Dataframe to be processed
    :param input_cols: Column to be processed
    :return:
    """

    # https://github.com/OpenRefine/OpenRefine/blob/master/main/src/com/google/refine/clustering/binning/FingerprintKeyer.java#L56
    def _split_sort_remove_join(value):
        """
        Helper function to split, remove duplicates, sort and join back together
        """
        # Split into whitespace-separated token
        # print("value", type(value), value)
        split_key = value.split()

        # Sort and remove duplicated items
        split_key = sorted(set(split_key))

        # join the tokens back together
        return "".join(split_key)

    input_cols = parse_columns(df, input_cols)
    for input_col in input_cols:
        output_col = name_col(input_col, FINGERPRINT_COL)
        df = (df
              .cols.trim(input_col, output_col)
              .cols.lower(output_col)
              .cols.remove_special_chars(output_col)
              .cols.normalize_chars(output_col)
              .cols.apply(output_col, _split_sort_remove_join, "string", mode="map")
              )
    return df


def n_gram_fingerprint(df, input_cols, n_size=2):
    """
    Calculate the ngram for a fingerprinted string
    :param df: Dataframe to be processed
    :param input_cols: Columns to be processed
    :param n_size:
    :return:
    """

    def calculate_ngrams(value, args):
        # remove white spaces
        ngram = list(ngrams(value, n_size))

        # sort and remove duplicated
        ngram = sorted(set(ngram))

        _result = ""
        for item in ngram:
            for i in item:
                _result = _result + i

        # join the tokens back together
        _result = "".join(_result)

        return _result

    input_cols = parse_columns(df, input_cols)

    for input_col in input_cols:
        ngram_fingerprint_col = name_col(input_col, FINGERPRINT_COL)
        # ngram_fingerprint_col = name_col(input_col, NGRAM_FINGERPRINT_COL)

        df = (df
              .cols.copy(input_col, ngram_fingerprint_col)
              .cols.lower(ngram_fingerprint_col)
              .cols.remove_white_spaces(ngram_fingerprint_col)
              .cols.apply(ngram_fingerprint_col, calculate_ngrams, "string", output_cols=ngram_fingerprint_col)
              .cols.remove_special_chars(ngram_fingerprint_col)
              .cols.normalize_chars(ngram_fingerprint_col)

              )

    return df


def fingerprint_cluster(df, input_cols, output: str = "dict"):
    return base_clustering_function(df, input_cols, output, func=fingerprint, args=[input_cols])


def n_gram_fingerprint_cluster(df, input_cols, n_size=2, output: str = "dict"):
    return base_clustering_function(df, input_cols, output, func=n_gram_fingerprint, args=[input_cols, n_size])


def base_clustering_function(df, input_cols, output, func=None, args=None):
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :return:
    """

    raise
