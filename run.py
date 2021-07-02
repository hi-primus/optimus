from flask import Flask
from flask import request, render_template

from optimus.api.code import generate_code

import json

app = Flask(__name__, template_folder="optimus/api/templates")

session = {}

def _run_code(body, run=False):

    global session

    code, updated = generate_code(body, list(session.keys()))

    res = {
        "status": "ok", 
        "code": code.split("\n")
    }

    if run:
        exec("result = None", session)

        try:
            exec("from optimus import Optimus", session)
            exec(code, session)
        except Exception as error:
            res.update({"error": str(error)})
            res.update({"status": "error"})
            
        result = eval("result or None", session)
        if result:
            res.update({"result": result})

    if updated and len(updated):
        res.update({"updated": updated})

    return res

@app.route('/run', methods=['POST'])
def run():
    body = json.loads(request.data)
    res = _run_code(body, True)
    return json.dumps(res)

@app.route('/code', methods=['POST'])
def code():
    body = json.loads(request.data)
    res = _run_code(body, False)
    return json.dumps(res)

@app.route('/initEngine', methods=['POST'])
def initEngine():
    body = json.loads(request.data)
    body["operation"] = "Optimus"
    res = _run_code(body, False)
    return json.dumps(res)

@app.route('/', methods=['GET'])
def playground():
    return render_template('playground.html')


