import sys
sys.path.append("../..")

df_dict = {('function', 'string'): ('a', 'b', 'c')}


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator
    op = Optimus("pandas")
    df = op.create.dataframe(df_dict)
    t = TestCreator(op, df, name="string")
    t.create(method="cols.upper", variant="multiple", cols=["function"])
    t.run()


create()
