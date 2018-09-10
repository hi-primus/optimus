import configparser
import json

from flask import Flask
from flask import jsonify

try:
    # try to load the config file
    config = configparser.ConfigParser()
    path = ""

    config.read("config.ini")
    path = config["SERVER"]["Input"]
    app = Flask(__name__)


    @app.route('/')
    def index():
        """
        Return a message indicating if the server is running.
        :return:
        """
        return jsonify("Optimus Server is running... Go to /profiler to get the Optimus profiler data.")


    @app.route('/profiler')
    def profiler():
        """
        Return the data profiler in json format.
        :return:
        """
        try:
            with app.app_context():
                with open(path, encoding="utf8") as f:
                    data = json.loads(f.read())
                    return jsonify(data)
        except IOError:
            return jsonify("Not data profiling available")


    app.run()

except IOError:
    print("config.ini not found")

except KeyError:
    print("Input info not found in config.ini. Be sure you have...")
    print("[SERVER]")
    print("Input = config.ini")
