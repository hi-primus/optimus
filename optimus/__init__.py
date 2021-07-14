import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root

from .optimus import optimus as Optimus
from optimus.version import __version__
# Handle encoding problem
# https://stackoverflow.com/questions/39662384/pyspark-unicodeencodeerror-ascii-codec-cant-encode-character

# Preserve compatibility with <3.x branch
os.environ["PYTHONIOENCODING"] = "utf8"



