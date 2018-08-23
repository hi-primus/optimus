from pyspark.ml import feature, classification
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, GBTClassifier

from optimus.helpers.checkit import is_dataframe
from optimus.helpers.functions import parse_columns
from optimus.ml.feature import string_to_index, vector_assembler


class ML:
    @staticmethod
    def logistic_regression_text(df, input_col):
        """
        Runs a logistic regression for input (text) DataFrame.
        :param df: Pyspark dataframe to analyze
        :param input_col: Column to predict
        :return: DataFrame with logistic regression and prediction run.
        """

        if not is_dataframe(df):
            raise TypeError("Spark dataframe expected")

        pl = feature.Tokenizer().setInputCol(input_col) | feature.CountVectorizer()
        ml = pl | classification.LogisticRegression()
        ml_model = ml.fit(df)
        df_model = ml_model.transform(df)
        return df_model, ml_model

    @staticmethod
    def random_forest(df, columns, input_col):
        """
        Runs a random forest classifier for input DataFrame.
        :param df: Pyspark dataframe to analyze.
        :param columns: List of columns to select for prediction.
        :param input_col: Column to predict.
        :return: DataFrame with random forest and prediction run.
        """

        if not is_dataframe(df):
            raise TypeError("Spark dataframe expected")

        columns = parse_columns(df, columns)

        assert isinstance(input_col, str), "Error, input column must be a string"

        data = df.select(columns)
        feats = data.columns
        feats.remove(input_col)

        df = string_to_index(df, input_cols=input_col)
        df = vector_assembler(df, input_cols=feats)

        model = RandomForestClassifier()

        df = df.cols.rename([(input_col + "_index", "label")])

        rf_model = model.fit(df)
        df_model = rf_model.transform(df)
        return df_model, rf_model

    @staticmethod
    def decision_tree(df, columns, input_col):
        """
        Runs a decision tree classifier for input DataFrame.
        :param df: Pyspark dataframe to analyze.
        :param columns: List of columns to select for prediction.
        :param input_col: Column to predict.
        :return: DataFrame with decision tree and prediction run.
        """

        if not is_dataframe(df):
            raise TypeError("Spark dataframe expected")

        columns = parse_columns(df, columns)

        assert isinstance(input_col, str), "Error, input column must be a string"

        data = df.select(columns)
        feats = data.columns
        feats.remove(input_col)

        df = string_to_index(df, input_cols=input_col)
        df = vector_assembler(df, input_cols=feats)

        model = DecisionTreeClassifier()

        df = df.cols.rename([(input_col + "_index", "label")])

        dt_model = model.fit(df)
        df_model = dt_model.transform(df)
        return df_model, dt_model

    @staticmethod
    def gbt(df, columns, input_col):
        """
        Runs a gradient boosting tree classifier for input DataFrame.
        :param df: Pyspark dataframe to analyze.
        :param columns: List of columns to select for prediction.
        :param input_col: Column to predict.
        :return: DataFrame with gradient boosting tree and prediction run.
        """

        if not is_dataframe(df):
            raise TypeError("Spark dataframe expected")

        columns = parse_columns(df, columns)

        assert isinstance(input_col, str), "Error, input column must be a string"

        data = df.select(columns)
        feats = data.columns
        feats.remove(input_col)

        df = string_to_index(df, input_cols=input_col)
        df = vector_assembler(df, input_cols=feats)

        model = GBTClassifier()

        df = df.cols.rename([(input_col + "_index", "label")])

        gbt_model = model.fit(df)
        df_model = gbt_model.transform(df)
        return df_model, gbt_model
