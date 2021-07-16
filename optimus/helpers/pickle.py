# Reference https://stackoverflow.com/questions/46692370/usage-of-custom-python-object-in-pyspark-udf
# def parse_to_inferred_types(df, col_name):
class test_pickle(object):
    def __call__(self, *args, **kwargs):
        print(args, kwargs)

