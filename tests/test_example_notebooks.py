# import platform
# import papermill as pm
# import numpy as np
#
# nan = np.nan
#
# notebooks = ["10_min_from_spark_to_pandas_with_optimus.ipynb",
#              "column.ipynb",
#              "row.ipynb",
#              "profiler.ipynb",
#              "plot.ipynb.ipynb",
#              "optimus.ipynb",
#              "hacking_optimus.ipynb",
#              "spark-optimus.ipynb",
#              "ml.ipynb",
#              "outliers.ipynb"]
#
#
# class Test_example_notebooks(object):
#     @staticmethod
#     def test_notebooks():
#         p = platform.system()
#         if p == "Linux" or p == "Darwin":
#             output = "/dev/null"
#         elif p == "Windows":
#             output = "$null"
#
#         for notebook in notebooks:
#             pm.execute_notebook(
#                 "/home/travis/build/ironmussa/Optimus/examples/" + notebook,
#                 output
#             )
