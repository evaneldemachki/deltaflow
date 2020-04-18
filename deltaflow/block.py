import numpy
import fastparquet
import pandas
from collections import OrderedDict
from pandas import DataFrame, Series, Int64Index, Index, RangeIndex
from typing import Union, Tuple, List, TypeVar, Iterable, Callable
from deltaflow.abstract import Selection
from deltaflow.errors import BlockError

DeltaReader = TypeVar('DeltaReader')
DeltaWriter = TypeVar('DeltaWriter')
Index = pandas.Index

type_map = {
    Int64Index: 'Int64Index',
    Index: 'Index',
    RangeIndex: 'RangeIndex',
    type(None): None
}

class Block:
    @staticmethod
    def parse(meta: dict, reader: DeltaReader) -> Tuple[DataFrame]:
        obj = fastparquet.ParquetFile('null', open_with=lambda *ignore: reader)
        reader.seek(0)
        obj = obj.to_pandas()
        # convert None values back to numpy.nan
        obj = obj.fillna(value=numpy.nan)
        # set index name back to None if index name is default
        if obj.index.name == 'index':
            obj.index.name = None

        return (obj,)

class AxisBlock(Block):
    __slots__ = ['drop', 'relabel', 'meta']
    def __init__(self, drop: Union[List, None], relabel: Union[List, None]):
        self.drop = drop if drop is not [None, None] else None
        self.relabel = relabel if relabel is not [None, None] else None

        structure = {}

        if self.drop is not None:
            structure['drop'] = [None, None]

            for axis in (0, 1):
                if self.drop[axis] is not None:
                    structure['drop'][axis] = {}
                    structure['drop'][axis]['shape'] = len(self.drop[axis])

        if self.relabel is not None:
            structure['relabel'] = [None, None]

            if self.relabel[0] is not None:
                index_name = self.relabel[0].name
            else:
                index_name = None

            for axis in (0, 1):
                if self.relabel[axis] is not None:
                    structure['relabel'][axis] = {}
                    structure['relabel'][axis]['shape'] = self.relabel[axis].shape[0]
                    structure['relabel'][axis]['type'] = type_map[type(self.relabel[axis])]
                    
                    self.relabel[axis] = self.relabel[axis].to_numpy()
        
            if index_name is not None:
                structure['relabel'][0]['name'] = index_name

        self.meta = {
            'class': 'axis',
            'structure': structure,
        }

    
    def write(self, writer: DeltaWriter) -> None:
        payload = {}
        axis_suffix = ['_rows', '_cols']
        if self.drop is not None:
            for obj, suffix in zip(self.drop, axis_suffix):
                if obj is not None:
                    payload['drop' + suffix] = obj
        if self.relabel is not None:
            for obj, suffix in zip(self.relabel, axis_suffix):
                if obj is not None:
                    payload['relabel' + suffix] = obj

        numpy.savez_compressed(writer, **payload)
        writer.next()
        self.meta['chunk'] = writer.push()
        
    @staticmethod
    def parse(meta: dict, reader: DeltaReader) -> OrderedDict:
        payload = numpy.load(reader, allow_pickle=True)
        obj = OrderedDict()

        structure = meta['structure']
        axis_suffix = ['_rows', '_cols']

        if 'drop' in structure:
            obj['drop'] = [None, None]
            for axis, suffix in zip((0, 1), axis_suffix):
                if structure['drop'][axis] is not None:
                    obj['drop'][axis] = payload['drop' + suffix]
                else:
                    obj['drop'][axis] = None
        
        if 'relabel' in structure:
            obj['relabel'] = [None, None]
            for axis, suffix in zip((0, 1), axis_suffix):
                if structure['relabel'][axis] is not None:
                    type_str = structure['relabel'][axis]['type']
                    if type_str == 'RangeIndex':
                        constructor = lambda item: pandas.RangeIndex(item[0], item[-1])
                    else:
                        constructor = getattr(pandas, type_str)

                    obj['relabel'][axis] = constructor(payload['relabel' + suffix])
                else:
                    obj['relabel'][axis] = None
            
            if structure['relabel'][0] is not None:
                if structure['relabel'][0]['name'] is not None:
                    obj['relabel'][0].name = structure['relabel'][0]['name']
        
        return obj
    
    @staticmethod
    def apply(meta: dict, obj: dict, data: DataFrame) -> DataFrame:
        if 'drop' in obj:
            for axis in (0, 1):
                if obj['drop'][axis] is not None:
                    data = data.drop(obj['drop'][axis], axis=axis)
        if 'relabel' in obj:
            for axis in (0, 1):
                if obj['relabel'][axis] is not None:
                    data = data.set_axis(obj['relabel'][axis], axis=axis)

        return data

    @staticmethod
    def stringify(entry: dict) -> List[str]:
        structure = entry['structure']
        obj_strings = []
        for key in ('drop', 'relabel'):
            if key in structure:
                out = key.upper() + ': '
                tags = []
                if structure[key][0] is not None:
                    tags.append("{0} row(s)".format(structure[key][0]['shape']))
                if structure[key][1] is not None:
                    tags.append("{0} column(s)".format(structure[key][1]['shape']))
        
                out += ', '.join(tags)
                obj_strings.append(out)

        return obj_strings

class PutBlock(Block):
    __slots__ = ['data', 'meta']
    def __init__(self, data: DataFrame, dtypes: Union[Series, None]):
        self.data = data
        
        # dtype preservation
        if dtypes is None:
            dt = None
        else:
            dt = dtypes.to_dict()
            dt = {col: str(dt[col]) for col in dt}

        self.meta = {
            'class': 'put',
            'method': 'put',
            'dtypes': dt,
            'shape': data.shape,
            'count': int(data.count().sum())
        }
    
    def write(self, writer: DeltaWriter) -> None:
        fastparquet.write('null', self.data, open_with=lambda *ignore: writer)
        writer.next()
    
        self.meta['chunk'] = writer.push()


    @staticmethod
    def apply(meta: dict, obj: Tuple[DataFrame], data: DataFrame) -> DataFrame:
        obj = obj[0].copy()
        data.update(obj)
        if meta['dtypes'] is not None:
            data = data.astype(meta['dtypes'])
        
        return data
    
    @staticmethod
    def stringify(entry):
        out = "PUT: {0} values".format(entry['count'])
        return [out]

class ExtensionBlock(Block):
    __slots__ = ['cols', 'rows', 'meta']
    def __init__(self, cols: DataFrame, rows: DataFrame):
        self.cols = cols
        self.rows = rows
        
        shape = []
        shape.append(self.cols.shape if cols is not None else None)
        shape.append(self.rows.shape if rows is not None else None)
        self.meta = {
            'class': 'extend',
            'shape': shape
        }
    
    def write(self, writer: DeltaWriter) -> None:
        chunks = []

        if self.cols is not None:
            fastparquet.write('null', self.cols, open_with=lambda *ignore: writer)
            writer.next()

        if self.rows is not None:
            fastparquet.write('null', self.rows, open_with=lambda *ignore: writer)
            writer.next()

        self.meta['chunk'] = writer.push()
    
    @staticmethod
    def parse(meta: dict, reader: DeltaReader) -> Tuple[DataFrame, None]:
        cols, rows = None, None
        if meta['shape'][0] is not None:
            cols = Block.parse(meta, reader)[0]
            reader.next()
        if meta['shape'][1] is not None:
            rows = Block.parse(meta, reader)[0]

        return cols, rows

    @staticmethod
    def apply(meta: dict, obj: Tuple[DataFrame, None], data: DataFrame) -> DataFrame:
        cols, rows = obj
        if cols is not None:
            data = pandas.concat([data, cols], axis=1)
        if rows is not None:
            data = pandas.concat([data, rows], axis=0)

        return data
    
    @staticmethod
    def stringify(entry):
        out = "EXTEND: "
        tags = []
        if entry['shape'][0] is not None:
            tags.append("{0} columns(s)".format(entry['shape'][0][1]))
        if entry['shape'][1] is not None:
            tags.append("{0} row(s)".format(entry['shape'][1][0]))
        
        out += ', '.join(tags)
        return [out]

block_map = {
    'axis': AxisBlock,
    'put': PutBlock,
    'extend': ExtensionBlock
}

def get_block(name: str) -> Block:
    return block_map[name]
