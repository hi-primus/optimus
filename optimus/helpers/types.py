from __future__ import annotations
import optimus.engines.base.engine as engine_module
import optimus.engines.base.basedataframe as dataframe_module
from typing import TypeVar, List

EngineType = TypeVar("EngineType", bound='engine_module.BaseEngine')
InternalEngineType = TypeVar("InternalEngineType")

DataFrameType = TypeVar("DataFrameType", bound='dataframe_module.BaseDataFrame')
InternalDataFrameType = TypeVar("InternalDataFrameType", bound=dict)
MaskDataFrameType = TypeVar("MaskDataFrameType", bound='dataframe_module.BaseDataFrame')
ConnectionType = TypeVar("ConnectionType")
ClustersType = TypeVar("ClustersType")
ModelType = TypeVar("ModelType")

DataFrameTypeList = TypeVar("DataFrameTypeList", bound=List['dataframe_module.BaseDataFrame'])
InternalDataFrameTypeList = TypeVar("InternalDataFrameTypeList", bound=list)
MaskDataFrameTypeList = TypeVar("MaskDataFrameTypeList", bound=list)
ConnectionTypeList = TypeVar("ConnectionTypeList", bound=list)
ClustersTypeList = TypeVar("ClustersTypeList", bound=list)
ModelTypeList = TypeVar("ModelTypeList", bound=list)

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
