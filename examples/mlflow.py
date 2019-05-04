# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# If you modify Optimus or any library this code is going to reload it
# %load_ext autoreload
# %autoreload 

# If you are in the example folder. This is the way to find optimus
import sys
sys.path.append("..")

# Create Optimus
from optimus import Optimus
op = Optimus()

import mlflow.sklearn
model = mlflow.sklearn.load_model(path="model", run_id="6a7ee9e4748f48c88fd06f2e76794d42") #Use one of the run IDs captured above
model.coef_

# !pip uninstall mlflow

# +
from sklearn import datasets
import numpy as np
import pandas as pd

# Load Diabetes datasets
diabetes = datasets.load_diabetes()
X = diabetes.data
y = diabetes.target

# Create pandas DataFrame for sklearn ElasticNet linear_model
Y = np.array([y]).transpose()
d = np.concatenate((X, Y), axis=1)
cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6', 'progression']
data = pd.DataFrame(d, columns=cols)
# -

# !pip install mlflow
# !pip install scikit-learn


