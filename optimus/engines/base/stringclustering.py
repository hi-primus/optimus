import copy
from optimus.engines.base.ml.contants import CLUSTER_COL, RECOMMENDED_COL
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_str


class Clusters:
    type = None
    clusters = {}
    
    
    def __init__(self, clusters):
        self.clusters = clusters


    def __repr__(self):
        return str(self.to_dict(verbose=True))


    def set_suggestions(self, suggestions, column=0):

        for i, suggestion in enumerate(suggestions):
            self.set_suggestion(i, suggestion, column)
        
    
    def set_suggestion(self, suggestion_or_id, new_value, column=0):

        if isinstance(column, (int,)):
            column = list(self.clusters.keys())[column]

        if is_str(suggestion_or_id):
            for i in range(len(self.clusters[column])):
                if (self.clusters[column][i]["suggestion"] == suggestion_or_id) or (self.clusters[column][i]["cluster"] == suggestion_or_id):
                    suggestion_or_id = i
                    break
            
            if is_str(suggestion_or_id):
                raise ValueError(f"'{suggestion_or_id}' is not a cluster of '{column}'")
                    
        self.clusters[column][suggestion_or_id]["suggestion"] = new_value
        
    
    def to_dict(self, columns="*", limit_clusters=None, limit_suggestions=None, verbose=False):
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


def string_clustering(df, input_cols, algorithm=None, *args, **kwargs):
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :return:
    """

    funcs = [ "fingerprint", "n_gram_fingerprint", "metaphone", "nysiis", "match_rating_codex",
              "double_methaphone", "soundex" ]

    if algorithm in funcs:
        func = getattr(df.cols, algorithm)
    else:
        RaiseIt.value_error(algorithm, funcs)

    input_cols = parse_columns(df, input_cols)

    result = {}

    for input_col in input_cols:

        cluster_col = name_col(input_col, CLUSTER_COL)
        df = func(input_col, output_cols=cluster_col, *args, **kwargs)

        # sets the original values as indices: {"VALUE": "value", "Value": "value"}

        _dfd = df.cols.select([input_col, cluster_col]).data.set_index(input_col)
        _df = df.new(_dfd)
        values = _df.to_pandas().to_dict()[cluster_col]

        # turns in into: {"value": ["VALUE", "Value"]}
        
        suggestions_items = {}
        for k, v in values.items():
            suggestions_items[v] = suggestions_items.get(v, []) + [k]

        counts_list = df.cols.frequency(cluster_col, len(values))['frequency'][cluster_col]['values']
        suggestions = []

        for d in counts_list:
            value = d['value']
            suggestion = { "suggestion": value, "cluster": value, "total_count": d['count'] }
            if suggestions_items[value]:
                suggestion["suggestions"] = suggestions_items[value]
                suggestion["suggestion"] = suggestion["suggestions"][0]
                suggestion["suggestions_size"] = len(suggestions_items[value])

            suggestions += [suggestion]

        result[input_col] = suggestions

    return Clusters(result)