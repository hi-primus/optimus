import json

import requests

HEADERS = {'Content-Type': 'application/json'}
START = {'kind': 'pyspark'}


class Livy:
    def __init__(self, host):
        self.host = host
        self.session_id = None
        self.session_url = None

    def start(self):
        """
        Start spark session
        :return:
        """
        self.session_url = self.host + '/sessions'
        r = self.send(self.session_url, data=START, type="POST")
        print(r)
        self.session_id = r['id']

    def session(self):
        """
        Get session info
        :return:
        """
        url = "{HOST}/sessions/{SESSION_ID}".format(HOST=self.host, SESSION_ID=self.session_id)
        return self.send(url, type="GET")

    def execute(self, data):
        """
        Sens a statemrn to livy
        :param data:
        :return:
        """

        url = "{HOST}/sessions/{SESSION_ID}/statements".format(HOST=self.host, SESSION_ID=self.session_id, timeout=10)

        return self.send(url, data=data, type="POST")

        # r.headers['location']

    @staticmethod
    def send(url, data=None, type="GET"):
        """
        Helper to manage http requests
        :param url:
        :param data:
        :param type:
        :return:
        """
        # url = self.host + '/sessions'

        try:
            if type == "GET":
                return requests.get(url, data=json.dumps(data), headers=HEADERS, verify=False, timeout=TIMEOUT).json()
            elif type == "POST":
                return requests.post(url, data=json.dumps(data), headers=HEADERS, verify=False, timeout=TIMEOUT).json()
            elif type == "DELETE":
                return requests.delete(url, data=json.dumps(data), headers=HEADERS, verify=False,
                                       timeout=TIMEOUT).json()

        except requests.exceptions.HTTPError as e:
            # Whoops it wasn't a 200
            print("Error: " + str(e))
            raise SystemExit(0)

    def finish(self):
        self.delete_session(self.session_id)

    def sessions(self):
        """
        Get all Spark SEssions
        :return:
        """

        r = self.send(self.session_url, type="GET")
        return r["sessions"]

    def delete_session(self, id=None):
        self.send("{SESSION_URL}/{SESSION_ID}".format(SESSION_URL=self.session_url, SESSION_ID=id),
                  type="DELETE")
