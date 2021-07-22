

from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler, StandardScaler

class DataFrameBaseFunctions():
    def standard_scaler(self, series):
        return StandardScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def max_abs_scaler(self, series):
        return MaxAbsScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def min_max_scaler(self, series):
        return MinMaxScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))
