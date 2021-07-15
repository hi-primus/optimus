import unittest
from optimus.helpers.functions import deep_sort
from optimus.helpers.json import json_enconding
from optimus import Optimus
import numpy as np
import sys
sys.path.append("..")
NaN = np.nan
null = None
false = False
true = True
op = Optimus("pandas")
source_df = op.create.dataframe(
    dict={('function', 'string'): ["a", "b", "c"]}, n_partitions=1)


class Test_string(unittest.TestCase):
    maxDiff = None

    def test_cols_upper_multiple(self):
        actual_df = source_df.cols.upper(cols=['function'])
        expected_df = op.create.dataframe(
            dict={('function', 'object'): ["A", "B", "C"]}, n_partitions=1)
        self.assertTrue(expected_df.equals(actual_df))
