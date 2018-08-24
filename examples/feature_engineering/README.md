# Predicting a customer's next purchase using automated feature engineering

<a style="margin:30px" href="https://www.featuretools.com">
    <img width=50% src="https://www.featuretools.com/wp-content/uploads/2017/12/FeatureLabs-Logo-Tangerine-800.png" alt="Featuretools" />
</a>

**As customers use your product, they leave behind a trail of behaviors that indicate how they will act in the future. Through automated feature engineering we can identify the predictive patterns in granular customer behavioral data that can be used to improve the customer's experience and generate additional revenue for your business.**

In this tutorial, we show how [Featuretools](https://www.featuretools.com) can be used to perform feature engineering on a multi-table dataset of 3 million online grocery orders provided by Instacart to train an accurate machine learning model to predict what product a customer buys next.

*Note: If you are running this notebook yourself, refer to the read me on Github for instructions to download the Instacart dataset*

## Highlights

* We automatically generate 150+ features using Deep Feature Synthesis and select the 20 most important features for predictive modeling
* We build a pipeline that it can be reused for numerous prediction problems (you can try this yourself!)
* We quickly develop a model on a subset of the data and validate on the entire dataset in a scalable manner using [Dask](http://dask.pydata.org/en/latest/).

## Read the tutorial

Link to notebook: [Tutorial](Tutorial.ipynb)


## Running the tutorial

1. Clone the repo

```
git clone https://github.com/Featuretools/predict_next_purchase.git
```

2. Install the requirements

```
pip install -r requirements.txt
```

3. Download the data

You can download the data directly from Instacart [here](https://www.instacart.com/datasets/grocery-shopping-2017).

After downloading the data save the CSVs to a directory called `data` in the root of this repository. Then run the following command in your terminal from the root of this repo.

```
>> python process_data.py
 70%|██████████████████████████▌           | 145/207 [07:43<03:18,  3.20s/it]
```

Expect this command to take up to 20 minutes to run as it prepares the data for the tutorial notebook

## Feature Labs


<a href="https://www.featurelabs.com/">
    <img src="http://www.featurelabs.com/wp-content/uploads/2017/12/logo.png" alt="Featuretools" />
</a>


Featuretools was created by the developers at [Feature Labs](https://www.featurelabs.com/). If building impactful data science pipelines is important to you or your business, please [get in touch](https://www.featurelabs.com/contact.html).
