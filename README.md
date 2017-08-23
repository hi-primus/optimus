[![Logo Optimus](https://github.com/ironmussa/Optimus/blob/master/images/logoOptimus.png)](https://github.com/ironmussa/Optimus)

[![PyPI version](https://badge.fury.io/py/optimuspyspark.svg)](https://badge.fury.io/py/optimuspyspark) [![Build Status](https://travis-ci.org/ironmussa/Optimus.svg?branch=master)](https://travis-ci.org/ironmussa/Optimus) [![Documentation Status](https://readthedocs.org/projects/optimus-ironmussa/badge/?version=latest)](http://optimus-ironmussa.readthedocs.io/en/latest/?badge=latest)
 [![built_by iron](https://img.shields.io/badge/built_by-iron-FF69A4.svg)](http://ironmussa.com) [![Updates](https://pyup.io/repos/github/ironmussa/Optimus/shield.svg)](https://pyup.io/repos/github/ironmussa/Optimus/)
 [![Python 3](https://pyup.io/repos/github/ironmussa/Optimus/python-3-shield.svg)](https://pyup.io/repos/github/ironmussa/Optimus/) [![GitHub release](https://img.shields.io/github/release/ironmussa/optimus.svg)](https://github.com/ironmussa/Optimus/) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/e01572e2af5640fcbcdd58e7408f3ea0)](https://www.codacy.com/app/favio.vazquezp/Optimus?utm_source=github.com&utm_medium=referral&utm_content=ironmussa/Optimus&utm_campaign=badger) 

[![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20Mac%20OS%20%7C%20Windows-blue.svg)](https://spark.apache.org/docs/2.2.0/#downloading) [![Dependency Status](https://gemnasium.com/badges/github.com/ironmussa/Optimus.svg)](https://gemnasium.com/github.com/ironmussa/Optimus)



## Click below for the official documentation

[![Documentation](https://media.readthedocs.com/corporate/img/header-logo.png)](http://optimus-ironmussa.readthedocs.io/en/latest/)

Optimus is the missing library for cleaning and pre-processing data in a distributed fashion. It uses all the power of Apache Spark (optimized via Catalyst) to do so. It implements several handy tools for data wrangling and munging that will make your life much easier. The first obvious advantage over any other public data cleaning library is that it will work on your laptop or your big cluster, and second, it is amazingly easy to install, use and understand.

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

## Announcement!!

We are very happy to annouce that Optimus main stable branch will 
work now for Spark 2.2.0 The 1.6.x version is now under maintenance, 
the last tag release for this Spark version is the 0.4.0. We strongly 
suggest that you use the >2.x version of the library because the 
new improvements and features will be added now on this version.

## Requirements
* Apache Spark 2.2.0
* Python 3.5

## Installation (pip):

In your terminal just type:

```
pip install optimuspyspark
```

## Contributors: 

 - Project Manager: [Argenis León](https://github.com/argenisleon) 
 - Original developers: [Andrea Rosales](https://github.com/andrearosr), [Hugo Reyes](https://github.com/hugounavez), [Alberto Bonsanto](https://github.com/Bonsanto)
 - Principal developer and maintainer: [Favio Vázquez](https://github.com/faviovazquez)
 
## License:

Apache 2.0 © [Iron](https://github.com/ironmussa)
 
