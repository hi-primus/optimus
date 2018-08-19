[![Logo Optimus](https://github.com/ironmussa/Optimus/blob/master/images/logoOptimus.png)](https://hioptimus.com)  
  
[![PyPI version](https://badge.fury.io/py/optimuspyspark.svg)](https://badge.fury.io/py/optimuspyspark) [![Build Status](https://travis-ci.org/ironmussa/Optimus.svg?branch=master)](https://travis-ci.org/ironmussa/Optimus) [![Documentation Status](https://readthedocs.org/projects/optimus-ironmussa/badge/?version=latest)](http://optimus-ironmussa.readthedocs.io/en/latest/?badge=latest)  
 [![built_by iron](https://img.shields.io/badge/built_by-iron-FF69A4.svg)](http://ironmussa.com) [![Updates](https://pyup.io/repos/github/ironmussa/Optimus/shield.svg)](https://pyup.io/repos/github/ironmussa/Optimus/)  
 [![GitHub release](https://img.shields.io/github/release/ironmussa/optimus.svg)](https://github.com/ironmussa/Optimus/) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/e01572e2af5640fcbcdd58e7408f3ea0)](https://www.codacy.com/app/favio.vazquezp/Optimus?utm_source=github.com&utm_medium=referral&utm_content=ironmussa/Optimus&utm_campaign=badger) [![StackShare](https://img.shields.io/badge/tech-stack-0690fa.svg?style=flat)](https://stackshare.io/iron-mussa/devops)  
[![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20Mac%20OS%20%7C%20Windows-blue.svg)](https://spark.apache.org/docs/2.2.0/#downloading) [![Code Health](https://landscape.io/github/ironmussa/Optimus/develop/landscape.svg?style=flat)](https://landscape.io/github/ironmussa/Optimus/develop) [![Coverage Status](https://coveralls.io/repos/github/ironmussa/Optimus/badge.svg?branch=master)](https://coveralls.io/github/ironmussa/Optimus?branch=master) [![Mentioned in Awesome Data Science](https://awesome.re/mentioned-badge.svg)](https://github.com/bulutyazilim/awesome-datascience)  
[![Join the chat at https://gitter.im/optimuspyspark/Lobby](https://badges.gitter.im/optimuspyspark/Lobby.svg)](https://gitter.im/optimuspyspark/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)  

Optimus is the missing framework to profile, clean, process and do ML in a distributed fashion using Apache Spark(PySpark).
Also in this new version Optimus can help you in the model creation with several tools like ...

## Installation (pip):  
  
In your terminal just type  `pip install optimuspyspark  `  
### Requirements
* Apache Spark 2.3.0  
* Python>=3.6  

## Examples

After installation go to the examples folder and found examples about data cleaning, data munging, and how to create ML and DL models.

## Documentation
  
[![Documentation](https://media.readthedocs.com/corporate/img/header-logo.png)](http://docs.hioptimus.com/en/latest/)  
  
## Feedback 
Feedback is what drive Optimus future. So take a couple of minutes to help shape the Optimus' Roadmap:  https://optimusdata.typeform.com/to/aEnYRY  

Also If you want to know what features are the most requested or have and idea you want to see in Optimus let us know at  
https://optimus.featureupvote.com/  
 
## Info
And if you want to see some cool information and tutorials for Optimus check out our blog https://medium.com/hi-optimus  
  
### Requirements
* Apache Spark 2.3.0  
* Python>=3.6  
  
## Basic Usage  
  
Optimus V2 was created to make data cleaning a brezee. The API was designed to be super easy to new commers and very familliar to the ones that comes from Pandas.
Optimus expand the Spark DataFrame functionality adding .rows and .cols attributes.

For example to:

`df = df\
	.cols.rename([('num','number')])\
    .cols.drop(["number","words"])\
    .withColumn("new_col_2", lit("spongebob"))\
    .cols.append("new_col_1", 1)\
    .cols.sort(order= "desc")\
    .table()`

Note that you can use Optimus functions and Spark functions(`.WithColumn()`) at the same time.

DataFrameTransformer class receives a dataFrame as an argument. This  
class has all methods listed above.  
 
## Contributing to Optimus
Contributions go far beyond pull requests and commits. We are very happy to receive any kind of contributions   
including:  
  
* [Documentation](https://github.com/ironmussa/Optimus/tree/master/docs/source) updates, enhancements, designs, or   bugfixes.  
* Spelling or grammar fixes.  
* README.md corrections or redesigns.  
* Adding unit, or functional [tests](https://github.com/ironmussa/Optimus/tree/master/tests)   
* Triaging GitHub issues -- especially determining whether an issue still persists or is reproducible.  
* [Searching #optimusdata on twitter](https://twitter.com/search?q=optimusdata) and helping someone else who needs help.  
* [Blogging, speaking about, or creating tutorials](https://hioptimus.com/category/blog/)   about Optimus and its many features.  
* Helping others in our optimus [gitter channel](https://gitter.im/optimuspyspark/Lobby).    
  
## Backers  
[[Become a backer](https://opencollective.com/optimus#backer)] and get your image on our README on Github with a link to your site.  
[![OpenCollective](https://opencollective.com/optimus/backers/badge.svg)](#backers)   
  
  
## Sponsors  
[[Become a sponsor](https://opencollective.com/optimus#backer)] and get your image on our README on Github with a link to your site.  
[![OpenCollective](https://opencollective.com/optimus/sponsors/badge.svg)](#sponsors)  
  
## Optimus for Spark 1.6.x  
  
Optimus main stable branch will work now for Spark 2.2.0 The 1.6.x version is now under maintenance, the last tag release for this Spark version is the 0.4.0. We strongly suggest that you use the >2.x version of the framework because the new improvements and features will be added now on this version.
## Core Team
Argenis Leon
Favio Vazquez

## Contributors:
Here is the amazing people that make Optimus possible:
  
[![0](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/0)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/0)  
[![1](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/1)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/1)  
[![2](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/2)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/2)  
[![3](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/3)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/3)  
[![4](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/4)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/4)  
[![5](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/5)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/5)  
[![6](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/6)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/6)  
[![7](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/7)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/7)  
  
[![Logo Data](https://www.bbvadata.com/wp-content/uploads/2016/07/bbvada_logo.png)](https://www.bbvadata.com)  
   
## License:  
  
Apache 2.0 © [Iron](https://github.com/ironmussa)  
  
[![Logo Iron](https://iron-ai.com/wp-content/uploads/2017/08/iron-svg-2.png)](https://ironmussa.com)  
  
<a href="https://twitter.com/optimus_data"><img src="https://www.shareicon.net/data/256x256/2015/09/01/94063_circle_512x512.png" alt="Optimus twitter" border="0" height="60"></a>