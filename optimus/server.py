import configparser
import json

from flask import Flask
from flask import jsonify

app = Flask(__name__)

config = configparser.ConfigParser()

# try to load the config file
try:
    config.read("config.ini")
except IOError:
    print("config not found")
    pass
path = config["SERVER"]["Input"]


@app.route('/')
def profiler_json():
    with app.app_context():
        with open(path, encoding="utf8") as f:
            data = json.loads(f.read())
            return jsonify(data)


if __name__ == '__main__':
    app.run()
