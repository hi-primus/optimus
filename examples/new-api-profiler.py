# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.2
#   kernel_info:
#     name: python3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.6.5
#   nteract:
#     version: 0.11.6
# ---

# %load_ext autoreload
# %autoreload 2

import sys
sys.path.append("..")

# ### Now you can get extra information for the profiler if you activate pass verbose= True to optimus

# Create optimus
from optimus import Optimus
op = Optimus(master="local[*]", app_name = "optimus" , checkpoint= True)

df = op.load.csv("data/Meteorite_Landings.csv").h_repartition()

df.table(10)

# ### Profiler dump mode (Faster). It just handle the column data type as present in the dataframe

op.profiler.run(df, "name", infer=False)

# ### Profiler smart mode (Slower). It just try to infer the column data type and present extra data acordinly. From example datetype columns get extra histograms about minutes, day, week and month. Also can detect array types on data.

op.profiler.run(df, "*",infer=True)

# ### Plot profile for a specific column

op.profiler.run(df, "reclat")

# ### Output a json file

# ### Plot histagram for multiple columns

df.plots.hist(["id", "reclong"], 20)

df.plots.frequency(["id", "reclong"], 10)

df.plot.correlation(["id","mass (g)", "reclat"])

df.correlation(["id","mass (g)", "reclat"], output="array")
