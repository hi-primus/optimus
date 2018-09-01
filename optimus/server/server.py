from optimus.server.process import Process
import os
import atexit


class Server:
    def __init__(self, path=None):
        if path is None:
            path = "python " + os.path.dirname(os.path.abspath(__file__)) + "\\run.py"

        self.process = None
        self.path = path

    def start(self):
        self.process = Process(self.path)

    def stop(self):
        self.process.stop()

    @atexit.register
    def goodbye(self):
        self.stop()
        print("You are now leaving the Python sector.")
