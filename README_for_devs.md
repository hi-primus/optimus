# Hacking Optimus

##Windows

If you want to contribute, hack or play with Optimus you are in the raight place. This is a little guide to

- Install Jupyter Notebooks

Download anaconda from https://www.anaconda.com/download/ and run the file

- Clone the Repo and install requirements

Go to the folder you want to download de repo and run:

`git clone https://github.com/ironmussa/Optimus.git`

The install the requirements

`pip install -r requirements.txt`

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

`setx SPARK_HOME C:\opt\spark\spark-2.3.1-bin-hadoop2.7`

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
