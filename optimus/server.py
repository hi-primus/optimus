import configparser
import json
from flask import Flask
from flask import jsonify
from multiprocessing import Process


class Server:
    def __init__(self):
        config = configparser.ConfigParser()

        self.path = ""
        # try to load the config file
        try:
            config.read("config.ini")
            self.path = config["SERVER"]["Input"]
        except IOError:
            print("config.ini not found")

        except KeyError:
            print("Input info not found in config.ini. Be sure you have...")
            print("[SERVER]")
            print("Input = config.ini")

        self.server = None

        app = Flask(__name__)

        @app.route('/')
        def index():
            """
            Return a message indicating if the server is running.
            :return:
            """

            return jsonify("Optimus Server si Running... Go to json /profiler to get the Optimus profiler data.")

        @app.route('/profiler')
        def profiler():
            """
            Return the data profiler in json format.
            :return:
            """
            try:
                with self.app.app_context():
                    with open(self.path, encoding="utf8") as f:
                        data = json.loads(f.read())
                        return jsonify(data)
            except IOError:
                return jsonify("Not data profiling available")

        self.app = app

    def start(self):
        """
        Start the Optimus Server
        :return:
        """

        # References
        # https://stackoverflow.com/questions/15562446/how-to-stop-flask-application-without-using-ctrl-c
        # https://stackoverflow.com/questions/33927616/multiprocess-within-flask-app-spinning-up-2-processes

        self.server = Process(target=self.app.run(debug=True, use_reloader=False))
        self.server.start()

        sys.stdout.flush()

        return

        # app.add_url_rule('/', 'index', index)
        # app.add_url_rule('/', "profiler ", profiler)

    def stop(self):
        """
        Stop the server
        :return:
        """
        self.server.terminate()
        self.server.join()
