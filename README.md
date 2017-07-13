# Optimus

[![Documentation Status](https://readthedocs.org/projects/optimus-ironmussa/badge/?version=latest)](http://optimus-ironmussa.readthedocs.io/en/latest/?badge=latest)

## Click below for the official documentation

[![Documentation](https://media.readthedocs.com/corporate/img/header-logo.png)](http://optimus-ironmussa.readthedocs.io/en/latest/)

Optimus is the missing library for cleaning and pre-processing data in a distributed fashion. It uses all the power of Apache Spark (optimized via Catalyst) to do it. It implements several handy tools for data wrangling and munging that will make your life much easier. The first obvious advantage over any other public data cleaning library is that it will work on your laptop or your big cluster, and second, it is amazingly easy to install, use and understand.

The following schema shows the structure class organization of the whole library:

<ul>
  <li> Optimus
      <ul>
          <li>DataFrameTransformer</li>
          <li>DataFrameAnalyzer</li>
      </ul>
  </li>

  <li>
      utilities
  </li>
</ul>

## Requirements
* Apache Spark 1.6
* Python 3.5

## Installation:
1 - Download `libs` folder and place the content inside your working folder.

2 - When starting pySpark in terminal, write the following line:
`$ pyspark --packages com.databricks:spark-csv_2.11:1.3.0 --py-files DfAnalizer.py,DfTransf.py,utilities.py`
