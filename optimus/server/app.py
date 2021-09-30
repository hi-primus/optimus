from optimus.server.functions import run, code, engine, session, create_session, delete_session, features
from flask import Flask
from flask import request, render_template

import json

app = Flask(__name__)

@app.route('/<session_key>/run', methods=['POST'])
def post__run(session_key):
    body = json.loads(request.data)
    res = run(session_key, body)
    return json.dumps(res)

@app.route('/<session_key>/code', methods=['POST'])
def post__code(session_key):
    body = json.loads(request.data)
    res = code(session_key, body)
    return json.dumps(res)

@app.route('/<session_key>/init-engine', methods=['POST'])
def post__init_engine(session_key):
    body = json.loads(request.data)
    res = engine(session_key, body or {"engine": "pandas"})
    return json.dumps(res)

@app.route('/<session_key>', methods=['GET'])
def get__session(session_key):
    res = session(session_key)
    return json.dumps(res)

@app.route('/<session_key>', methods=['POST'])
def post__create_session(session_key):
    res = create_session(session_key)
    return json.dumps(res)

@app.route('/<session_key>', methods=['DELETE'])
def delete__session(session_key):
    res = delete_session(session_key)
    return json.dumps(res)

@app.route('/features', methods=['GET'])
def get__features():
    return json.dumps(features())

@app.route('/', methods=['GET'])
def get__playground():
    return render_template('playground.html')