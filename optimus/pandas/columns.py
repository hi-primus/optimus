def cols(self):
    class Cols:
        @staticmethod
        def date_transform():
            raise NotImplementedError('Look at me I am pandas now')

        @staticmethod
        def names():
            return list(self.columns)

    return Cols()


from pandas import DataFrame

DataFrame.cols = property(cols)
