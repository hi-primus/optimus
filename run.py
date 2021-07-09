from optimus.api.functions import get_optimus_features, run_or_code
from flask import Flask
from flask import request, render_template

import json

app = Flask(__name__, template_folder="optimus/api/templates")

optimus_features = None
sessions = {}

def _run_or_code(session_key, body, run=False):

    global sessions
    print(type(body))

    session = sessions.get(session_key, {})

    res, session = run_or_code(session, body, run)

    if session_key != "default":
        res.update({"session": session_key})

    sessions.update({session_key: session})

    return res

@app.route('/<session_key>/run', methods=['POST'])
def run(session_key):
    body = json.loads(request.data)
    res = _run_or_code(session_key, body, True)
    return json.dumps(res)

@app.route('/<session_key>/code', methods=['POST'])
def code(session_key):
    body = json.loads(request.data)
    res = _run_or_code(session_key, body, False)
    return json.dumps(res)

@app.route('/<session_key>/init-engine', methods=['POST'])
def init_engine(session_key):
    body = json.loads(request.data)
    body["operation"] = "Optimus"
    res = _run_or_code(session_key, body, False)
    return json.dumps(res)

@app.route('/<session_key>', methods=['GET'])
def session(session_key):
    global sessions
    
    if session_key is None:
        res = {"status": "error", "content": "Session not found in request", "code": 400}
    else:
        session = sessions.pop(session_key, None)
        if session is None:
            res = {"status": "error", "content": "Session not found", "code": 404}
        else:
            res = {"status": "ok", "content": "Session found", "code": 200}

    return json.dumps(res)

@app.route('/<session_key>', methods=['POST'])
def create_session(session_key):
    global sessions
    
    if session_key is None:
        res = {"status": "error", "content": "Session not found in request", "code": 400}
    else:
        session = sessions.pop(session_key, None)
        if session is None:
            sessions.update({session: {}})
            res = {"status": "ok", "content": "Session created", "code": 200}
        else:
            res = {"status": "ok", "content": "Session already created", "code": 200}

    return json.dumps(res)

@app.route('/<session_key>', methods=['DELETE'])
def delete_session(session_key):
    global sessions
    body = json.loads(request.data)
    session_key = body.pop("session", None)
    
    if session_key is None:
        res = {"status": "error", "content": "Session not found in request", "code": 400}
    else:
        session = sessions.pop(session_key, None)
        if session is None:
            res = {"status": "error", "content": "Session not found", "code": 404}
        else:
            del sessions[session_key]
            res = {"status": "ok", "content": "Session deleted", "code": 200}

    return json.dumps(res)

@app.route('/features', methods=['GET'])
def features():
    
    global optimus_features

    if optimus_features is None:
        optimus_features = get_optimus_features()

    return json.dumps(optimus_features)
    

@app.route('/', methods=['GET'])
def playground():
    return render_template('playground.html')


