import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator, default_configs

    op = Optimus("pandas")
    df = op.create.dataframe({
        ('name', 'string'): ('Optimus', 'Bumblebee', 'Eject'),
        ('age (M)', 'int'): [5, 5, 5]
    })

    t = TestCreator(op, df, name="example", configs=default_configs)
    t.create(method="cols.upper", variant="single", cols=["name"])
    t.create(method="cols.upper", variant="multiple", compare_by="json", cols=["name", "age (M)"])
    t.run()


create()
