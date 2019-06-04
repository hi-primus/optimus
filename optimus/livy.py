import json
import textwrap

import requests

HEADERS = {'Content-Type': 'application/json'}
START = {'kind': 'pyspark'}
TIMEOUT = 10


# Reference https://github.com/apache/incubator-livy/tree/master/python-api/src/main/python/livy

class Livy:
    def __init__(self, host):
        self.host = host
        self.session_id = None
        self.session_url = None
        self.statement_id = None

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

    def submit(self, code):
        """
        Send code to livy
        :param code:
        :return:
        """

        data = {'code': textwrap.dedent(code)}

        url = "{HOST}/sessions/{SESSION_ID}/statements".format(HOST=self.host, SESSION_ID=self.session_id)

        r = self.send(url, data=data, type="POST")

        self.statement_id = r["id"]

        filter_string = ["id", "output", "progress", "state"]
        return {k: v for (k, v) in r.items() if k in filter_string}
        # r.headers['location']

    def result(self, id=None):
        """

        :param id:
        :return:
        """
        if id is None:
            id = self.statement_id
        url = "{HOST}/sessions/{SESSION_ID}/statements/{STATEMENT_ID}".format(HOST=self.host,
                                                                              SESSION_ID=self.session_id,
                                                                              STATEMENT_ID=id)

        r = self.send(url, type="GET")
        if r["output"]["status"] == "ok":
            return r["output"]["data"]["text/plain"]

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
