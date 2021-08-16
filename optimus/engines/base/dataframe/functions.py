

from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler, StandardScaler

from optimus.engines.base.functions import BaseFunctions


class DataFrameBaseFunctions(BaseFunctions):

    def reverse(self, series):
        return self.to_string(series).map(lambda v: v[::-1])

    def standard_scaler(self, series):
        return StandardScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def max_abs_scaler(self, series):
        return MaxAbsScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def min_max_scaler(self, series):
        return MinMaxScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))
