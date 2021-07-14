import uuid
import os

from optimus.helpers.functions import prepare_path


def getfile_and_infer(path):
    temp_filename = str(uuid.uuid4())

    # local_filename = urllib.request.urlretrieve(path, temp_filename)
    # print(local_filename)

    full_file_name, file_name = prepare_path(path)

    # temp_filename = "data/crime.csv"

    file_ext = os.path.splitext(file_name)[1]

    file = open(full_file_name).read(2048)

    import magic
    mime, encoding = magic.Magic(mime=True, mime_encoding=True).from_file(full_file_name).split(";")
    mime_info = {"mime": mime, "encoding": encoding.strip().split("=")[1], "ext": file_ext}

    xml_threshold = 10
    json_threshold = 20

    if mime == "text/plain":

        # Try to infer if is a valid json
        if sum([file.count(i) for i in ['(', '{', '}', '[', ']']]) > json_threshold:
            mime_info["filetype"] = "json"

        elif sum([file.count(i) for i in ['<', '/>']]) > xml_threshold:
            mime_info["filetype"] = "xml"

        # CSV
        else:
            try:
                import csv
                dialect = csv.Sniffer().sniff(file)
                mime_info["filetype"] = "csv"

                r = {"properties": {"delimiter": dialect.delimiter,
                                    "doublequote": dialect.doublequote,
                                    "escapechar": dialect.escapechar,
                                    "lineterminator": dialect.lineterminator,
                                    "quotechar": dialect.quotechar,
                                    "quoting": dialect.quoting,
                                    "skipinitialspace": dialect.skipinitialspace}}
            except:
                pass

            mime_info.update(r)
    elif mime == "application/vnd.ms-excel":
        mime_info["filetype"] = "excel"

    return mime_info
