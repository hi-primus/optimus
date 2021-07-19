import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator

    op = Optimus("pandas")
    df = op.create.dataframe({('name', 'string'): (
        'Optimus', 'Bumblebee', 'Eject'), ('age (M)', 'int'): [5, 5, 5]})

    configs = {
        "Pandas": {"engine": "pandas"},
        "Dask": {"engine": "dask", "n_partitions": 1},
        "Dask2": {"engine": "dask", "n_partitions": 2}
    }

    t = TestCreator(op, df, name="example", configs=configs)
    t.create(method="cols.upper", variant="single", cols=["name"])
    t.create(method="cols.upper", variant="multiple",
             compare_by="json", cols=["name", "age (M)"])
    t.run()


create()
