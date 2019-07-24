import base64
import json
import os
import pprint
from io import BytesIO

from IPython.core.display import display, HTML
from matplotlib import pyplot as plt

from optimus.helpers.check import is_str


def output_image(fig, path):
    """
    Output a png file
    :param fig:
    :param path: Matplotlib figure
    :return: Base64 encode image
    """

    fig.savefig(path, format='png')
    plt.close()


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


def print_html(html):
    """
    Display() helper to print html code
    :param html: html code to be printed
    :return:
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        displayHTML(result)
    else:
        display(HTML(html))


def print_json(value):
    """
    Print a human readable json
    :param value: json to be printed
    :return: json
    """
    pp = pprint.PrettyPrinter(indent=2)
    if is_str(value):
        value = value.replace("'", "\"")
        value = json.loads(value)

    pp.pprint(value)
