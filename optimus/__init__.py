import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root
from .optimus import Optimus

os.environ["PYTHONIOENCODING"] = "utf8"
