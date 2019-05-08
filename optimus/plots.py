import base64
from io import BytesIO

import altair as alt
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from numpy import array

from optimus.functions import ellipsis


def plot_correlation(column_data):
    """
    Plot a correlation plot
    :param column_data:
    :return:
    """
    return sns.heatmap(column_data, mask=np.zeros_like(column_data, dtype=np.bool),
                       cmap=sns.diverging_palette(220, 10, as_cmap=True))


def plot_hist(column_data=None, output=None, sub_title=""):
    """
    :param column_data:
    :param output:
    :param sub_title:
    :return:
    """

    for col_name, data in column_data.items():
        # Plot
        return alt.Chart.from_dict(data)

        # Save as base64
        # if output is "base64":
        #     return output_base64(fig)


def plot_missing_values(column_data=None, output=None):
    """
    Plot missing values
    :param column_data:
    :param output: image o base64
    :return:
    """
    values = []
    columns = []
    labels = []
    for col_name, data in column_data["data"].items():
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

    plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

    # Save as base64
    if output is "base64":
        return output_base64(fig)


def plot_freq(column_data=None, output=None):
    """
    Frequency plot
    :param column_data: column data in json format
    :param output: image or base64
    :return:
    """

    for col_name, data in column_data.items():

        # Transform Optimus' format to matplotlib's format
        # x = []
        # h = []
        #
        # for d in data:
        #     x.append(ellipsis(d["value"]))
        #     h.append(d["count"])
        return alt.Chart.from_dict(data)

        # Save as base64
        if output is "base64":
            return output_base64(fig)


def plot_boxplot(column_data=None, output=None):
    """
    Boxplot
    :param column_data: column data in json format
    :param output: image or base64
    :return:
    """
    for col_name, stats in column_data.items():
        fig, axes = plt.subplots(1, 1)

        bp = axes.bxp(stats, patch_artist=True)

        axes.set_title(col_name)
        plt.figure(figsize=(12, 5))

        # 'fliers', 'means', 'medians', 'caps'
        for element in ['boxes', 'whiskers']:
            plt.setp(bp[element], color='#1f77b4')

        for patch in bp['boxes']:
            patch.set(facecolor='white')

            # Tweak spacing to prevent clipping of tick-labels
        plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

        # Save as base64
        if output is "base64":
            return output_base64(fig)


def plot_scatterplot(column_data=None, output=None):
    """
    Boxplot
    :param column_data: column data in json format
    :param output: image or base64
    :return:
    """

    fig = plt.figure(figsize=(12, 5))
    plt.scatter(column_data["x"]["data"], column_data["y"]["data"], s=column_data["s"], alpha=0.5)
    plt.xlabel(column_data["x"]["name"])
    plt.ylabel(column_data["y"]["name"])

    # Tweak spacing to prevent clipping of tick-labels
    # plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

    # Save as base64
    if output is "base64":
        return output_base64(fig)


def output_base64(fig):
    """
    Output a matplotlib as base64 encode
    :param fig: Matplotlib figure
    :return: Base64 encode image
    """
    fig_file = BytesIO()
    plt.savefig(fig_file, format='png')

    # rewind to beginning of file
    fig_file.seek(0)

    fig_png = base64.b64encode(fig_file.getvalue())
    plt.close(fig)

    return fig_png.decode('utf8')
