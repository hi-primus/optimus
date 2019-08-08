# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernel_info:
#     name: python3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %load_ext autoreload
# %autoreload 2

import sys
sys.path.append("..")

# ### Now you can get extra information for the profiler if you activate pass verbose= True to optimus

# Create optimus
from optimus import Optimus
op = Optimus(master="local[*]", app_name = "optimus" , checkpoint= True, verbose=True)

df = op.load.csv("data/Meteorite_Landings.csv").h_repartition()

df.table(10)

# ### Profiler dump mode (Faster). It just handle the column data type as present in the dataframe

op.profiler.run(df, "name", infer=False, approx_count= True)

# ### Profiler smart mode (Slower). It just try to infer the column data type and present extra data accordingly. From example datetype columns get extra histograms about minutes, day, week and month. Also can detect array types on data.

op.profiler.run(df, "GeoLocation",infer=True)

# ### Plot profile for a specific column

op.profiler.run(df, "reclat")

# ### Output a json file

# ### Plot histagram for multiple columns

df.plot.hist(["id", "reclong"], 20)

df.plot.frequency(["id", "reclong"], 10)

df.table()

df.cols.count_na("*")

a = {'name': 0,
 'id': 0,
 'nametype': 0,
 'recclass': 0,
 'mass (g)': 131,
 'fall': 0,
 'year': 288,
 'reclat': 7315,
 'reclong': 7315,
 'GeoLocation': 7315}

    df.cols.dtypes()

# +
cols = ["id","mass (g)","reclat"]
# We drops nulls because correlation can not handle them
df_not_nulls = df.rows.drop_na(cols)

df_not_nulls.plot.correlation(cols)
# -

df_not_nulls.cols.correlation(["id","mass (g)", "reclat"], output="array")


