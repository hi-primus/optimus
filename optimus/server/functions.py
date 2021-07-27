from optimus.server.code import generate_code

optimus_features = None
sessions = {}

def get_optimus_features():
    try:
        import pyspark
        spark_available = True
    except:
        spark_available = False

    try:
        import coiled
        coiled_available = True
    except:
        coiled_available = False

    try:
        import dask
        dask_available = True
    except:
        dask_available = False

    try:
        import cudf
        import dask_cudf
        rapids_available = True
    except:
        rapids_available = False

    coiled_gpu_available = coiled_available

    res = {
        "coiled_available": coiled_available,
        "coiled_gpu_available": coiled_gpu_available,
        "spark_available": spark_available,
        "dask_available": dask_available,
        "rapids_available": rapids_available
    }

    try:
        from optimus.expressions import reserved_words
        res.update({'reserved_words': reserved_words})
    except:
        pass

    return res

def run_or_code(session, body, run=False):
    
    code, updated = generate_code(body, list(session.keys()), True)

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

    return res, session

def _run_or_code_request(session_key, body, run=False):

    global sessions


    session = sessions.get(session_key, {})
    res, session = run_or_code(session, body, run)

    if session_key != "default":
        res.update({"session": session_key})

    sessions.update({session_key: session})
    return res

def run(session_key, body):
    return _run_or_code_request(session_key, body, True)

def code(session_key, body):
    return _run_or_code_request(session_key, body, False)

def engine(session_key, body):
    body.update({"operation": "Optimus"})
    return _run_or_code_request(session_key, body, True)

def session(session_key):
    global sessions
    
    if session_key is None:
        return {"status": "error", "content": "Session not found in request", "code": 400}
    else:
        session = sessions.pop(session_key, None)
        if session is None:
            return {"status": "error", "content": "Session not found", "code": 404}
        else:
            return {"status": "ok", "content": "Session found", "code": 200}

def create_session(session_key):
    global sessions
    
    if session_key is None:
        return {"status": "error", "content": "Session not found in request", "code": 400}
    else:
        session = sessions.pop(session_key, None)
        if session is None:
            sessions.update({session: {}})
            return {"status": "ok", "content": "Session created", "code": 200}
        else:
            return {"status": "ok", "content": "Session already created", "code": 200}

def delete_session(session_key):
    global sessions
    
    if session_key is None:
        return {"status": "error", "content": "Session not found in request", "code": 400}
    else:
        session = sessions.pop(session_key, None)
        if session is None:
            return {"status": "error", "content": "Session not found", "code": 404}
        else:
            del sessions[session_key]
            return {"status": "ok", "content": "Session deleted", "code": 200}

def features():
    global optimus_features
    if optimus_features is None:
        optimus_features = get_optimus_features()
    return optimus_features
