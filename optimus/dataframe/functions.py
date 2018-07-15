from pyspark.sql import DataFrame
from optimus.helpers.decorators import *


@add_attr(DataFrame)
def execute(self):
    """
    This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
    evaluation approach in processing data: transformation functions are not computed into an action is called.
    Sometimes when transformations are numerous, the computations are very extensive because the high number of
    operations that spark needs to run in order to get the results.

    Other important thing is that apache spark usually save task but not result of dataFrame, so tasks are
    accumulated and the same situation happens.

    The problem can be deal it with the checkPoint method. This method save the resulting dataFrame in disk, so
     the lineage is cut.
    """

    # Checkpointing of dataFrame. One question can be thought. Why not use cache() or persist() instead of
    # checkpoint. This is because cache() and persis() apparently do not break the lineage of operations,
    print("Saving changes at disk by checkpoint...")
    df = self

    df.checkpoint()
    df.count()
    # self = self._sql_context.createDataFrame(self, self.schema)
    print("Done.")

    return None

# @add_attr(DataFrame)
# def head(self):
#    self.show()
