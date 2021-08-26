import numpy as np
import seaborn as sns
import statsmodels.api as sm
from matplotlib import pyplot as plt
from numpy.core._multiarray_umath import array

from optimus.infer import is_dict, is_list
from optimus.helpers.core import one_list_to_val
from optimus.helpers.functions import ellipsis
from optimus.helpers.output import output_image, output_base64, print_html


def plot_heatmap(column_data=None, output=None, path=None):
    """
    Scatter plot
    :param column_data: column data in json format
    :param output: image or base64
    :param path:
    :return:
    """

    fig = plt.figure(figsize=(12, 5))
    extent = column_data["x"]["edges"] + column_data["y"]["edges"]
    plt.imshow(column_data["values"], extent=extent, origin='lower')
    plt.xlabel(column_data["x"]["name"])
    plt.ylabel(column_data["y"]["name"])

    if output == "base64":
        return output_base64(fig)
    elif output == "image":
        output_image(fig, path)
        print_html("<img src='" + path + "'>")
    elif output == "plot":
        # Tweak spacing to prevent clipping of tick-labels
        plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)


def plot_boxplot(column_data=None, output=None, path=None):
    """
    Box plot
    :param column_data: column data in json format
    :param output: image, base64 or plot. Image output a file, base64 output a base64 encoded image and plot output the
    image to the notebook
    :param path:
    :return:
    """
    for col_name, stats in column_data.items():
        if not is_dict(stats):
            continue
        fig, axes = plt.subplots(1, 1)
        stats["whislo"] = stats.pop("whisker_low")
        stats["whishi"] = stats.pop("whisker_high")
        stats["med"] = stats.pop("median")
        bp = axes.bxp([stats], patch_artist=True)

        axes.set_title(col_name)
        plt.figure(figsize=(12, 5))

        # 'fliers', 'means', 'medians', 'caps'
        for element in ['boxes', 'whiskers']:
            plt.setp(bp[element], color='#1f77b4')

        for patch in bp['boxes']:
            patch.set(facecolor='white')

            # Tweak spacing to prevent clipping of tick-labels

        # Save as base64
        if output == "base64":
            return output_base64(fig)
        elif output == "image":
            output_image(plt, path)
            print_html("<img src='" + path + "'>")


def plot_frequency(column_data=None, output=None, path=None):
    """
    Frequency plot
    :param column_data: column data in json format
    :param output: image, base64 or plot. Image output a file, base64 output a base64 encoded image and plot output the
    image to the notebook
    :param path:
    :return:
    """
    for col_name, data in column_data.items():
        if not is_dict(data):
            continue

        # Transform Optimus' format to matplotlib's format
        x = []
        h = []
        for _data in data.values():
            for _item in _data:
                x.append(ellipsis(_item["value"]))
                h.append(_item["count"])

        # Plot
        fig = plt.figure(figsize=(12, 5))

        # Need to to this to plot string labels on x
        x_i = range(len(x))
        plt.bar(x_i, h)
        plt.xticks(x_i, x)

        plt.title("Frequency '" + col_name + "'")

        plt.xticks(rotation=45, ha="right")
        plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

        if output == "base64":
            return output_base64(fig)
        elif output == "image":
            output_image(plt, path)
            print_html("<img src='" + path + "'>")
        elif output == "plot":
            # Tweak spacing to prevent clipping of tick-labels
            plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)


def plot_hist(column_data=None, output=None, sub_title="", path=None):
    """
    Plot a histogram
    obj = {"col_name":[{'lower': -87.36666870117188, 'upper': -70.51333465576172, 'value': 0},
    {'lower': -70.51333465576172, 'upper': -53.66000061035157, 'value': 22094},
    {'lower': -53.66000061035157, 'upper': -36.80666656494141, 'value': 2},
    ...
    ]}
    :param column_data: column data in json format
    :param output: image, base64 or plot. Image output a file, base64 output a base64 encoded image and plot output the
    image to the notebook
    :param sub_title: plot subtitle
    :param path:
    :return: plot, image or base64
    """

    for col_name, data in column_data.items():
        if not is_list(data):
            continue
        bins = []
        for d in data:
            bins.append(d['lower'])

        last = data[len(data) - 1]["upper"]
        bins.append(last)

        # Transform hist Optimus format to matplot lib format
        hist = []
        for d in data:
            if d is not None:
                hist.append(d["count"])

        array_bins = array(bins)
        center = (array_bins[:-1] + array_bins[1:]) / 2
        width = 0.9 * (array_bins[1] - array_bins[0])

        hist = one_list_to_val(hist)

        # Plot
        fig = plt.figure(figsize=(12, 5))
        plt.bar(center, hist, width=width)
        plt.title("Histogram '" + col_name + "' " + sub_title)

        # fig.tight_layout()

        if output == "base64":
            return output_base64(fig)
        elif output == "image":
            # Save image
            output_image(plt, path)
            print_html("<img src='" + path + "'>")
            # Print in jupyter notebook

        elif output == "plot":
            plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)


def plot_correlation(cols_data, output=None, path=None):
    """
    Plot a correlation plot
    :param cols_data:
    :param output:
    :param path:
    :return:
    """
    import pandas as pd
    df = pd.DataFrame(data=cols_data)

    sns_plot = sns.heatmap(df, cmap=sns.diverging_palette(220, 10, as_cmap=True), annot=True)

    if output == "base64":
        # fig = sns.get_figure()
        fig = sns_plot.get_figure()
        return output_base64(fig)
    elif output == "image":
        # Save image
        fig = sns_plot.get_figure()
        fig.savefig(path)
        print_html("<img src='" + path + "'>")


def plot_missing_values(column_data=None, output=None, path=None):
    """
    Plot missing values
    :param column_data:
    :param output: image, base64 or plot. Image output a file, base64 output a base64 encoded image and plot output the
    :param path:
    image to the notebook
    :return:
    """
    values = []
    columns = []
    labels = []
    for col_name, data in column_data["data"].items():
        if not is_dict(data):
            continue
        values.append(data["missing"])
        columns.append(col_name)
        labels.append(data["%"])

    # Plot
    fig = plt.figure(figsize=(12, 5))
    plt.bar(columns, values)
    plt.xticks(columns, columns)

    # Highest limit
    highest = column_data["count"]
    plt.ylim(0, 1.05 * highest)
    plt.title("Missing Values")
    i = 0
    for label, val in zip(labels, values):
        plt.text(x=i - 0.5, y=val + (highest * 0.05), s="{}({})".format(val, label))
        i = i + 1

    if output == "base64":
        return output_base64(fig)
    elif output == "image":
        output_image(plt, path)
    elif output == "plot":
        plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)


def plot_qqplot(col_name, sample_data, output="plot", path=None):
    """
    Plot a qqplot
    :param col_name:
    :param sample_data:
    :param output:
    :param path:
    :return:
    """
    fig = plt.figure(figsize=(12, 5))

    sm.qqplot(sample_data.toPandas()[col_name], line='q', color='C0', alpha=0.3)

    plt.title("qqplot '" + col_name + "' ")

    if output == "base64":
        return output_base64(fig)
    elif output == "image":
        output_image(plt, path)
