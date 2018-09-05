import logging
import os
import signal

from psutil import NoSuchProcess

from optimus.server.process import Process


class Server:
    def __init__(self, path=None):
        if path is None:
            path = "python " + os.path.dirname(os.path.abspath(__file__)) + "\\run.py"

        self.process = None
        self.path = path
        self.pid = None
        self.pid_file = "server.pid"
        signal.signal(signal.SIGINT, self.stop)

    def start(self):
        """
        Start the Optimus Server
        :return:
        """

        pid_file = self.pid_file

        # Verify if server.pid exist
        if os.path.isfile(pid_file):
            pid = int(open(pid_file, 'r').read())
            logging.info("Server seems to be running with process id {pid}".format(pid=pid))
            self.pid = pid

        else:
            # Start the server
            process = Process(self.path)
            pid = process.id
            logging.info("Server started with process id " + str(pid))
            open(pid_file, 'w').write(str(pid))
            self.pid = pid

    def stop(self):
        """
        Stop the Optimus Server
        :return:
        """
        try:
            Process.stop_id(self.pid)
            logging.info("Optimus Server stopped")
        except (ProcessLookupError, NoSuchProcess):
            os.remove(self.pid_file)
            logging.info("Optimus could not be stopped. Process id not found")
