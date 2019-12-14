# Hacking Optimus

## Windows

If you want to contribute, hack or play with Optimus you are in the right place. This is a little guide to

- Install Jupyter Notebooks

Download anaconda from https://www.anaconda.com/download/ and run the file

- Clone the Repo and install requirements

Go to the folder you want to download de repo and run:

```
git clone https://github.com/ironmussa/Optimus.git
```

The install the requirements

```
pip install -r requirements.txt
```

- Install Spark

Go to http://spark.apache.org/downloads.html and download Spark 2.3.1 with Pre-built Hadoop 2.7
Decompress the file in c:\opt\spark

- Install java

First, install chocolatey https://chocolatey.org/install#installing-chocolatey

> > Chocolatey needs Administrative permission.

Then from the command line
`choco install jdk8`

## Set vars

Be sure that the Spark version is the same that you download

```
setx SPARK_HOME C:\opt\spark\spark-2.3.1-bin-hadoop2.7
```

Check in the console if python is run as 'python','python3' etc. Use the one you found to set PYSPARK_PYTHON
`setx PYSPARK_PYTHON python`

Open the examples folder in the cloned Optimus repo and go to hack_optimus.ipyn. There you are going fo find some tips
to hack and contribute with Optimus

## Troubleshooting

Error: you do not have permission to tmp\hive
from anaconda
wintools.exe ls c:\tmp\hive
wintools.exe chmod 777 c:\tmp\hive

## Processing CSS
To recreate the templates you need to inline the CSS.  

First go to https://nodejs.org and download and install nodejs

From the optimus repo in the terminal:

- Run `npm install`
- Run `node inlinecss.js`

## Testing packages

Sometimes we need to test and modify some files a make test in some environments for example Google DataProc or Databricks.
For that we could upload pip package to test.pypi.org so we could test it in the remote environment faster without touching the main repo.

First create the pip package. A `dist` folder should be create

```
python setup.py sdist bdist_wheel
```

Then upload to test.pypi.org. Be sure to create and account 
```
pip install twine
```

This will prompt your test.pypi.org credentials

```
python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

Now install from test.pypi.org
```
!pip install --index-url https://test.pypi.org/simple optimuspyspark
```

### Installing from github

```
pip3 install --upgrade --no-deps --force-reinstall git+https://github.com/ironmussa/Optimus.git@develop
```

### Compiling redis .jar file
This file can not be found to download and must be compiled.

#### Install Maven

Reference https://www.javahelps.com/2017/10/install-apache-maven-on-linux.html

#### Compile the .jar file
Download spark-redis from GitHub https://github.com/RedisLabs/spark-redis (either git clone or download as a zip), and build it using Maven.

```
$ cd spark-redis
$ mvn clean package -DskipTests
```
