import configparser
import json
from pathlib import Path

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

print(Path.cwd())


@app.route('/')
def profiler_json():
    with app.app_context():
        with open(path) as f:
            data = json.load(f)
            return jsonify(data)


if __name__ == '__main__':
    app.run()


class Server:
    def __init__(self, output_path=None):
        self.path = output_path
        self.output_path = output_path

        app = Flask("optimus")
        app.run(debug=False)
        app.route('/')(self.output_json)
        self.app = app

        # path = Path.cwd() / "data.json"

    def output_json(self):
        with open(self.path) as f:
            data = json.load(f)
        return jsonify(data)
