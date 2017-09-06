# Importing DataFrameTransformer library
from optimus.df_transformer import DataFrameTransformer
# Importing DataFrameAnalyzer library
from optimus.df_analyzer import DataFrameAnalyzer
# Importing DfProfiler library
from optimus.df_analyzer import DataFrameProfiler
# Importing Utility library
from optimus.utilities import *
# Import Outliers library
from optimus.df_outliers import *
# Import iPython
from IPython.display import display, HTML


def printHTML(html):
    display(HTML(html))


message = "Optimus successfully imported. Have fun :)"

printHTML(
    """
    <div style="margin:10px">
        <a href="https://github.com/ironmussa/Optimus" target="_new">
            <img src="http://optimus-ironmussa.readthedocs.io/en/latest/_images/logoOptimus.png" style="float:left;margin-right:10px" height="50" width="50"/>
        </a>
        <span>{0}</span>
    </div>
    """.format(message)
)

# module level doc-string
__doc__ = """
Optimus = Optimus is the missing framework for cleansing (cleaning and much more), pre-processing and exploratory data 
analysis in a distributed fashion with Apache Spark.
=====================================================================
Optimus the missing framework for cleansing (cleaning and much more), pre-processing and exploratory data analysis in a 
distributed fashion. It uses all the power of Apache Spark to do so. It implements several handy tools for data 
wrangling and munging that will make your life much easier. The first obvious advantage over any other public data 
cleaning library is that it will work on your laptop or your big cluster, and second, it is amazingly easy to 
install, use and understand.
"""
