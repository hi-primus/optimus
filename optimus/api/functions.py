from optimus.api.code import generate_code


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