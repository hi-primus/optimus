class PandasEngine:
    def __init__(self, *args, **kwargs):
        self.engine = 'pandas'

    def create(self, data):
        import pandas as pd
        return pd.DataFrame(data)