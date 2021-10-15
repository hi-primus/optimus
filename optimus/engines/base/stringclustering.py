import copy
from itertools import combinations_with_replacement
from optimus.helpers.types import *

import jellyfish as jellyfish
import numpy as np

from optimus.engines.base.ml.constants import CLUSTER_COL
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.output import output_json
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list, is_str


class Clusters:
    type = None
    clusters = {}

    def __init__(self, clusters):
        self.clusters = clusters

    def __repr__(self):
        return str(output_json(self.to_dict(verbose=True)))

    def set_suggestions(self, suggestions, column=0):

        for i, suggestion in enumerate(suggestions):
            self.set_suggestion(i, suggestion, column)

    def set_suggestion(self, suggestion_or_id, new_value, column=0):

        if isinstance(column, (int,)):
            column = list(self.clusters.keys())[column]

        if is_str(suggestion_or_id):
            for i in range(len(self.clusters[column])):
                if (self.clusters[column][i]["suggestion"] == suggestion_or_id) or (
                        self.clusters[column][i]["cluster"] == suggestion_or_id):
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
            clusters = self.clusters[column]
            if not is_list(clusters):
                clusters = list(clusters.values())
                
            for cluster in clusters[0:limit_clusters]:
                if verbose:
                    cluster_name = cluster["cluster"]
                    _cluster = copy.deepcopy(cluster)
                    del _cluster["cluster"]
                    result[column][cluster_name] = _cluster
                else:
                    cluster_name = cluster["suggestion"]
                    result[column][cluster_name] = result[column].get(cluster_name, [])
                    result[column][cluster_name] += cluster["suggestions"][0:limit_suggestions]

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

    def display(self, columns="*", limit_clusters=None, limit_suggestions=None, verbose=True):
        return self.to_dict(columns, limit_clusters, limit_suggestions, verbose)


def string_clustering(df, cols="*", algorithm=None, *args, **kwargs) -> 'ClustersType':
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :return:
    """

    funcs = ["fingerprint", "ngram_fingerprint", "metaphone", "nysiis", "match_rating_codex",
             "double_metaphone", "soundex", "levenshtein"]

    if algorithm in funcs:
        func = getattr(df.cols, algorithm)
    else:
        RaiseIt.value_error(algorithm, funcs)

    cols = parse_columns(df, cols)
    result = {}

    for input_col in cols:
        if algorithm == "levenshtein":
            a = np.empty((0, 3))
            _df = df[input_col].rows.drop_duplicated(how="all")
            _df = df.cols.fingerprint(input_col)

            for b, z in zip(combinations_with_replacement(_df.data[input_col], 2),
                            combinations_with_replacement(_df.data[input_col], 2)):
                a = np.append(a, np.array([[b[0], b[1], jellyfish.levenshtein_distance(*z)]], dtype="object"), axis=0)

            total_rows = df.rows.count()
            topn = 2
            result = {}
            suggestions = []

            for s in _df.data[input_col]:
                b = a[((a[:, 0] == s) | (a[:, 1] == s))]

                c = b[b[:, 2].argsort()][:topn][:, [0, 1]]
                c = [s] + c[np.where(c != s)].tolist()
                suggestions.append({
                    "cluster": s,
                    "suggestion": c[0], 
                    "suggestions": c, 
                    "suggestions_size": len(c), 
                    "total_count": total_rows
                })
            result[input_col] = suggestions
        else:

            cluster_col = name_col(input_col, CLUSTER_COL)
            # TODO: Think we can remove duplicated values to improve time calculation performace
            df = func(input_col, output_cols=cluster_col, *args, **kwargs)

            # sets the original values as indices: {"VALUE": "value", "Value": "value"}

            _dfd = df.cols.select([input_col, cluster_col]).cols.to_string().data.set_index(input_col)
            _df = df.new(_dfd)
            values = _df[cluster_col].to_pandas().to_dict()[cluster_col]

            suggestions_items = {}
            for k, v in values.items():
                suggestions_items[v] = suggestions_items.get(v, []) + [k]

            counts_list = df.cols.frequency(cluster_col, len(values))['frequency'][cluster_col]['values']
            suggestions = []

            for d in counts_list:
                value = d['value']
                suggestion = {"suggestion": value, "cluster": value, "total_count": d['count']}
                if suggestions_items[value]:
                    suggestion["suggestions"] = suggestions_items[value]
                    suggestion["suggestion"] = suggestion["suggestions"][0]
                    suggestion["suggestions_size"] = len(suggestions_items[value])

                suggestions += [suggestion]

            result[input_col] = suggestions
    return Clusters(result)
