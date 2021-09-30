import os
from .optimus import optimus as Optimus
from optimus._version import __version__

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root


# Preserve compatibility with <3.x branch
os.environ["PYTHONIOENCODING"] = "utf8"
