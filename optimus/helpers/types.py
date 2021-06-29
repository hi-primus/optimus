
from typing import TypeVar, List

DataFrameType = TypeVar("DataFrameType")
ConnectionType = TypeVar("ConnectionType")
ClustersType = TypeVar("ClustersType")

StringsList = TypeVar("StringsList", List[str], str)
StringsListNone = TypeVar("StringsListNone", List[str], str, None)
