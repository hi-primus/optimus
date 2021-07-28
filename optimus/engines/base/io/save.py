import warnings
from optimus.helpers.types import *


class BaseSave:
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def file(self, path:str, *args, **kwargs):
        if "." not in path:
            warnings.warn("No file extension found in path, saving to Parquet file.")
            file_ext = "parquet"

        file_ext = path.split(".")[-1]

        funcs = {
            'xls': 'excel',
            'xlsx': 'excel'
        }

        func_name = funcs.get(file_ext, file_ext.lower())

        func = getattr(self, func_name, None)

        if not callable(func):
            raise ValueError(f"No function found fot extension '{file_ext}'")

        return func(path, *args, **kwargs)
    
    def csv(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
    
    def xml(self, filename: str=None, mode: str='w'):
        dfd = self.root.data

        def row_to_xml(row):
            xml = ['<item>']
            for i, col_name in enumerate(row.index):
                xml.append('  <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
            xml.append('</item>')
            return '\n'.join(xml)

        res = '\n'.join(dfd.apply(row_to_xml, axis=1))

        if filename is None:
            return res

        folder_name = "/".join(filename.split('/')[0:-1])

        from pathlib import Path
        Path(folder_name).mkdir(parents=True, exist_ok=True)

        with open(filename, mode) as f:
            f.write(res)
    
    def json(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def excel(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def avro(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
    
    def parquet(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def orc(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def hdf5(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def database_table(self, table, db, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
