from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import NGram
import string


class KeyCollision():
    def __init__(self, df):
        self.df = df

    def __validate__(self, column):
        # Asserting data variable is string:
        assert type(column) == type('s'), "Error: Column argument must be a string."

        # If None or [] is provided with column parameter:
        assert column != "", "Error: Column can not be a empty string"

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.df.dtypes)]

        # asserts the column parameter is a string column of the dataFrame
        assert column in validCols, 'Error: Columns or column does not exist in the dataFrame or is numberic column'

    """
    Cluster a dataframe column based on the Fingerprint algorithm
    """

    def __cluster_fingerprints__(self, column, outputCol, sort_tokens, remove_duplicates):
        def removeAndSort(string, sort_tokens, remove_duplicates):
            split_key = string.split()

            # remove duplicates, if chosen
            if remove_duplicates:
                dups_removed = set(split_key)
            else:
                dups_removed = split_key

            # sort the tokens, if chosen
            if sort_tokens:
                # sort the tokens
                sorted_split_key = sorted(dups_removed)
            else:
                sorted_split_key = dups_removed

            # join the tokens back together
            return " ".join(sorted_split_key)

        def rmvSpecialChars(inputStr):
            for punct in (set(inputStr) & set(string.punctuation)):
                inputStr = inputStr.replace(punct, "")
            return inputStr

        fCol = outputCol
        clustering = (self.df
                      .groupBy(column)
                      .count()
                      .select('count', column)
                      .withColumn(fCol, self.df[column])
                      .cache())

        #         remove leading and trailing whitespace
        #         change all characters to their lowercase representation
        #         remove all punctuation and control characters
        #         split the string into whitespace-separated tokens
        #         sort the tokens and remove duplicates
        #         join the tokens back together

        exprs = [trim(col(c)).alias(c) if c == fCol else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        exprs = [lower(col(c)).alias(c) if c == fCol else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        func = udf(lambda cell: rmvSpecialChars(cell) if cell != None else cell, StringType())
        exprs = [func(col(c)).alias(c) if c == fCol else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        removeAndSortFunc = lambda cell: removeAndSort(cell, sort_tokens, remove_duplicates) if cell != None else cell
        func = udf(removeAndSortFunc, StringType())
        exprs = [func(col(c)).alias(c) if c == fCol else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        clustering.unpersist()
        return clustering

    """
    Cluster a DataFrame column based on the N-Gram Fingerprint algorithm
    """

    def __cluster_ngram__(self, column, n_size, outputCol):
        def joinUnique(seq):
            seen = set()
            x = [x for x in seq if not (x in seen or seen.add(x))]
            return "".join(x).replace(' ', '')

        def rmvSpecialChars(inputStr):
            for punct in (set(inputStr) & set(string.punctuation)):
                inputStr = inputStr.replace(punct, "")
            return "".join(inputStr.split())

        newColumn = column + "Changes"
        nGramCol = outputCol

        clustering = (self.df.select(column)
                      .groupBy(column)
                      .count()
                      .withColumn(newColumn, self.df[column])
                      .cache())

        #         change all characters to their lowercase representation
        #         remove all punctuation, whitespace, and control characters
        #         obtain all the string n-grams
        #         sort the n-grams and remove duplicates
        #         join the sorted n-grams back together

        exprs = [lower(col(c)).alias(c) if c == newColumn else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        func = udf(lambda cell: rmvSpecialChars(cell), StringType())
        exprs = [func(col(c)).alias(c) if c == newColumn else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        exprs = [trim(col(c)).alias(c) if c == newColumn else c for c in clustering.columns]
        clustering = clustering.select(*exprs)

        # create NGram object
        ngram = NGram(n=n_size, inputCol=newColumn, outputCol=nGramCol)

        # flatten newColumn string values and ngram transform the column
        exprs = [split(col(c), '').alias(c) if c == newColumn else c for c in clustering.columns]
        ngram_df = ngram.transform(clustering.select(*exprs))

        # sort and remove duplicate ngrams
        clustering = (ngram_df
                      .select(*['count', column, sort_array(nGramCol)
                              .alias(nGramCol)]))

        # join sorted, unique ngrams back together
        function = udf(joinUnique, StringType())
        exprs = [function(col(c)).alias(c) if c == nGramCol else c for c in clustering.columns]
        clustering = (clustering.select(*exprs))

        clustering.unpersist()
        return clustering

    def __clustering__(self, clusterDF, column, outputCol):
        # find the most frequent item per fingerprint
        cluster_keys = (clusterDF.select('*')
                        .join(clusterDF
                              .groupBy(outputCol)
                              .agg(max('count')
                                   .alias('count'))
                              , ['count', outputCol]))

        cluster_keys_dict = (cluster_keys
                             .map(lambda f: f[outputCol])
                             .zip(cluster_keys.map(lambda f: f[column]))
                             .collectAsMap())

        # crea un mapa con el formato: {most_frequent_item: [items_in_cluster]}

        clustering = (clusterDF.map(lambda r: r[outputCol])
                      .zip(clusterDF.map(lambda f: f[column]))
                      .map(lambda x: (x[0], [x[1]]))
                      .reduceByKey(lambda a, b: a + b)
                      .filter(lambda x: len(x[1]) > 1)
                      .collectAsMap())

        return dict([(cluster_keys_dict.get(key), value) for key, value in clustering.items()])

    """
    Returns a clustered dataframe column as a dataframe with columns [strings = String, fingerprints = String]
    """

    def fingerprint(self, column, outputCol="Fingerprint", sort_tokens=False, remove_duplicates=False):
        self.__validate__(column)

        return self.__cluster_fingerprints__(column, outputCol, sort_tokens, remove_duplicates)

    """
    Returns a clustered dataframe column as a dataframe with columns [strings = String, nGrams = String]
    """

    def nGramFingerprint(self, column, n_size=2, outputCol="nGram"):
        self.__validate__(column)

        return self.__cluster_ngram__(column, n_size, outputCol)

    """
    Returns the clustered dataframe column as a Python list to provide iterative properties
    """

    def fingerprintCluster(self, column, outputCol='Fingerprint', sort_tokens=False, remove_duplicates=False):
        def list_by_fingerprint(column, outputCol, sort_tokens, remove_duplicates):
            clusterDF = self.__cluster_fingerprints__(column, outputCol, sort_tokens, remove_duplicates)

            return self.__clustering__(clusterDF, column, outputCol)

        self.__validate__(column)

        return list_by_fingerprint(column, outputCol, sort_tokens, remove_duplicates)

    """
    Returns the clustered dataframe column as a Python list to provide iterative properties
    """

    def nGramFingerprintCluster(self, column, n_size=2, outputCol='nGram'):
        def list_by_fingerprint(column, n_size, outputCol):
            clusterDF = self.__cluster_ngram__(column, n_size, outputCol)

            return __clustering__(clusterDF, column, outputCol)

        self.__validate__(column)

        return list_by_fingerprint(column, n_size, outputCol)