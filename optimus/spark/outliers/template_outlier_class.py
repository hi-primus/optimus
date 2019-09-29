# TODO: We shold implement this has and interface
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

        # Calculate the outliers to filter the rows

        # Return here the df the outliers rows
        return df

    def drop(self):
        """
        This must drop the rows with outliers
        :return:
        """
        df = self.df

        # Calculate the outliers to filter the rows

        # Return here the df without the outliers rows
        return df

    def count(self):
        """
        Count the outliers rows using the selected column
        :return:
        """

        return self.df.select().count()

    def non_outliers_count(self):
        """
        Count non outliers rows using the selected column
        :return:
        """

        return self.drop().count()

    def info(self):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """

        return {}
