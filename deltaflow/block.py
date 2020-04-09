import pandas
import numpy
from typing import Union
from deltaflow.errors import BlockError

PandasIndex = Union[pandas.Int64Index, pandas.Index, pandas.RangeIndex]

type_to_str = {
    pandas.Int64Index: 'Int64Index',
    pandas.Index: 'Index',
    pandas.RangeIndex: 'RangeIndex'
}

class Block:
    @staticmethod
    def unwrap(meta, payload):
        # convert None values back to numpy.nan
        obj = payload.fillna(value=numpy.nan)
        # set index name back to None if index name is default
        if obj.index.name == 'index':
            obj.index.name = None

        return obj

class IndexBlock(Block):
    __slots__ = ['payload', 'meta']
    def __init__(self, index: PandasIndex, axis: int, method: str):
        name = 'index' if index.name is None else index.name
        self.payload = pandas.DataFrame(
            index.to_numpy(), columns=[name])
        
        self.meta = {
            'class': 'index',
            'method': method,
            'axis': axis,
            'type': type_to_str[type(index)],
            'shape': index.shape
        }
    
    @staticmethod
    def unwrap(meta: dict, payload: pandas.DataFrame) -> PandasIndex:
        constructor = getattr(pandas, meta['type'])
        name = payload.columns[0]
        if type(constructor) is pandas.RangeIndex:
            index = payload[name].to_numpy()
            imin, imax = index.min(), index.max()
            if name == 'index':
                obj = constructor(imin, imax)
            else:
                obj = constructor(imin, imax, name=name)
        else:
            index = payload[name].to_numpy()
            if name == 'index':
                obj = constructor(index)
            else:
                obj = constructor(index, name=name)
        
        return obj
    
    @staticmethod
    def apply(meta: dict, obj: PandasIndex, data: pandas.DataFrame) -> pandas.DataFrame:
        if meta['method'] == 'set':
            data = data.set_axis(obj, axis=meta['axis'])
        elif meta['method'] == 'drop':
            data = data.drop(obj, axis=meta['axis'])
        elif meta['method'] == 'sort':
            if meta['axis'] == 0:
                data = data.reindex(index=obj)
            else:
                data = data.reindex(index=None, columns=obj)
        else:
            msg = "invalid IndexBlock method '{0}'"
            raise BlockError(msg.format(meta['method']))

        return data


class PutBlock(Block):
    __slots__ = ['payload', 'meta']
    def __init__(self, data: pandas.DataFrame, dtypes: Union[None, pandas.Series]):
        self.payload = data
        
        if dtypes is None:
            self.meta = {'class': 'put'}
        else:
            dt = dtypes.to_dict()
            dt = {col: str(dt[col]) for col in dt}
            self.meta = {
                'class': 'put',
                'dtypes': dt,
                'shape': data.shape,
                'count': int(data.count().sum())
            }
    
    @staticmethod
    def apply(meta: dict, obj: pandas.DataFrame, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.copy()
        data.update(obj)
        if 'dtypes' in meta:
            data = data.astype(meta['dtypes'])
        
        return data

class ExtensionBlock(Block):
    __slots__ = ['payload', 'meta']
    def __init__(self, data: pandas.DataFrame, axis: int):
        self.payload = data
        
        self.meta = {
            'class': 'extension',
            'axis': axis,
            'shape': data.shape
        }
    
    @staticmethod
    def apply(meta: dict, obj: pandas.DataFrame, data: pandas.DataFrame) -> pandas.DataFrame:
        data = pandas.concat([data, obj], axis=meta['axis'])
        return data

class_map = {
    'index': IndexBlock,
    'put': PutBlock,
    'extension': ExtensionBlock
}