import os
from .optimus import optimus as Optimus
from optimus._version import __version__

ROOT_DIR = os.path.dirname(os.path.abspath(
    __file__))  # This is your Project Root


# Preserve compatibility with <3.x branch
os.environ["PYTHONIOENCODING"] = "utf8"

# module level doc-string
__doc__ = """
🚚 Agile Data Preparation Workflows made easy with Pandas, Dask, cuDF, Dask-cuDF and PySpark

Usage
-----
`from optimus import Optimus

op = Optimus("pandas")`
"""
