from typing import TypeVar, List

DataFrameType = TypeVar("DataFrameType")
InternalDataFrameType = TypeVar("InternalDataFrameType")
MaskDataFrameType = TypeVar("MaskDataFrameType")
ConnectionType = TypeVar("ConnectionType")
ClustersType = TypeVar("ClustersType")
ModelType = TypeVar("ModelType")

DataFrameTypeList = TypeVar("DataFrameTypeList")
InternalDataFrameTypeList = TypeVar("InternalDataFrameTypeList")
MaskDataFrameTypeList = TypeVar("MaskDataFrameTypeList")
ConnectionTypeList = TypeVar("ConnectionTypeList")
ClustersTypeList = TypeVar("ClustersTypeList")
ModelTypeList = TypeVar("ModelTypeList")

StringsList = TypeVar("StringsList", List[str], str)
StringsListNone = TypeVar("StringsListNone", List[str], str, None)

_list_types = [str(DataFrameTypeList), str(InternalDataFrameTypeList), str(MaskDataFrameTypeList), str(ConnectionTypeList), str(ClustersTypeList), str(ModelTypeList)]

_types = [str(DataFrameType), str(InternalDataFrameType), str(MaskDataFrameType), str(ConnectionType), str(ClustersType), str(ModelType)]

def is_list_of_optimus_type(value):
    return str(value) in _list_types

def is_optimus_type(value):
    return str(value) in _types

def is_any_optimus_type(value):
    return str(value) in [*_types, *_list_types]