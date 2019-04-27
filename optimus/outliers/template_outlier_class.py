class TemplateOutlierClass:
    """
    This is a template class to expand the outliers methods
    Also you need to add the function to outliers.py
    """

    def __init__(self, df, columns, any_param):
        """

        :param df:
        :param columns:
        :param any_param:
        """
        self.df = df
        self.columns = columns
        self.any_param = any_param

    def select(self):
        """
        This must return the rows
        :return:
        """

        df = self.df

        # Calculate the outliers with the dataframe

        # Return here the df the outliers rows
        return df

    def drop(self):
        """
        This must drop the rows with outliers
        :return:
        """
        df = self.df

        # Calculate the outliers with the dataframe

        # Return here the df without the outliers rows
        return df
