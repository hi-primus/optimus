from abc import ABC, abstractmethod


class AbstractOutlierThreshold(ABC):
    """
     This is a template class to expand the outliers methods
     Also you need to add the function to outliers.py
     """

    def select(self):
        """
        Select outliers rows using the selected column
        :return:
        """
        df = self.df
        z_score = self.z_score

        return df.rows.select(
            (z_score > self.threshold) | (z_score < (self.threshold) * -1))

    def select_lower_bound(self):
        df = self.df
        z_score = self.z_score
        return df.rows.select((z_score < (self.threshold) * -1))

    def select_upper_bound(self):
        df = self.df
        z_score = self.z_score
        return df.rows.select((z_score > (self.threshold)))

    def drop(self):
        """
        Drop outliers rows using the selected column
        :return:
        """

        df = self.df
        z_score = self.z_score
        return df.rows.drop(z_score.cols.abs() >= self.threshold)

    def count_lower_bound(self, bound: int):
        """
        Count outlier in the lower bound
        :return:
        """
        col_name = self.col_name
        return self.df.rows.select(self.df[col_name] < bound).count()

    def count_upper_bound(self, bound: int):
        """
        Count outliers in the upper bound
        :return:
        """
        col_name = self.col_name
        return self.df.rows.select(self.df[col_name] >= bound).count()

    def count(self):
        """
        Count the outliers rows using the selected column
        :return:
        """
        df = self.z_score
        return df.rows.select(df > self.threshold).rows.count(compute=False)

    def non_outliers_count(self):
        """
        Count non outliers rows using the selected column
        :return:
        """
        df = self.df
        z_score = self.z_score

        return df.rows.select(z_score < self.threshold).rows.count(compute=False)

    @abstractmethod
    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        pass
