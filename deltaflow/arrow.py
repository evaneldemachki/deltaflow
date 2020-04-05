import os
import pandas
from collections import OrderedDict
from typing import TypeVar, Union, Iterable, Any, Tuple
from deltaflow.errors import (
    UndoError, IndexerError, IntegrityError, 
    AxisLabelError, InsertionError, 
    ExtensionError, ObjectTypeError,
    AxisOverlapError, DataTypeError
)
from deltaflow import fs
from deltaflow.hash import hash_data, hash_pair, hash_node
from deltaflow.delta import Delta, block_function
from deltaflow.node import Node
import deltaflow.operation as op

Tree = TypeVar('Tree')
Operation = op.Operation
PandasObject = Union[pandas.DataFrame, pandas.Series]
RowIndex = Union[pandas.RangeIndex, pandas.Int64Index]
ColIndex = pandas.Index
DataFrameIndexer = Union[PandasObject, RowIndex, ColIndex, Iterable, int, str]

class Layer:
    def __init__(self):
        self.batch = []
    
    def push(self, data: pandas.DataFrame, operation: Operation) -> pandas.DataFrame:
        self.batch.append(operation)
        queue = operation.execute(data)
        return queue

class Stage:
    def __init__(self, data: pandas.DataFrame):
        self.base = data
        self.data = data.copy()
        self.stack = []

    def add(self, layer: Layer) -> None:
        self.stack.append(layer)
    
    def revert(self) -> None:
        data = self.data.copy()
        try:
            layer = self.stack[-1]
            for oper in reversed(layer.batch):
                data = oper.undo(data)
            
            self.stack.pop()
        except IndexError:
            raise UndoError

        self.data = data
    
    # iterate through stack layers as flat sequence of operations
    def iter_operations(self):
        for layer in self.stack:
            for oper in layer.batch:
                yield oper
    
    def __str__(self):
        out = '[\n'
        for layer in self.stack:
            if len(layer.batch) == 1:
                out += '  ' + layer.batch[0].display() + ',\n'
            else:
                out += '  [\n'
                for oper in layer.batch:
                    out += '    ' + oper.display() + ',\n'
                
                out += '  ]'
            
        out += ']'
        return out
    
    __repr__ = __str__

        
class Arrow:
    def __init__(self, tree, name):
        node_id = tree.arrow_head(name)
        self.name = name
        self.head = tree.node(node_id)

        timeline = tree.timeline(node_id)
        origin_id = next(iter(timeline))
        origin_name = tree.name_origin(origin_id)

        self._timeline = timeline
        self._tree = tree
        self._origin = (origin_name, origin_id)

        self.stage = Stage(self._resolve_timeline())

    def proxy(self):
        return self.stage.data.copy()

    def undo(self):
        self.stage.revert()
        return self.proxy()

    # update records in a segment of current stage dataset with
    # ... corresponding proxy records of matching indices
    def update(self, data: PandasObject):
        # determine columns that intersect with stage
        stage_columns = self.stage.data.columns
        update_cols = stage_columns.intersection(op.column_index(data))
        # determine row indices that intersect with stage
        stage_index = pandas.Int64Index(self.stage.data.index)
        update_rows = stage_index.intersection(data.index)
        # select update segments
        stage = self.stage.data.loc[update_rows, update_cols]
        data = data.loc[update_rows, update_cols]
        # assure update segments have matching dtypes
        if (stage.dtypes.to_numpy() != data.dtypes.to_numpy()).any():
            raise DataTypeError
        # shrink modifications in both directions
        x = op.shrink(data, stage)
        y = op.shrink(stage, data)
        # if layer is empty, return proxy as is
        if y.shape[0] == 0:
            return self.proxy()
            
        layer = Layer()
        self.stage.data = layer.push(self.stage.data, 
            op.Update(x, y, stage.dtypes))

        self.stage.add(layer)
        return self.proxy()
  
    def drop(self, indexer: DataFrameIndexer, axis: int = 0) -> pandas.DataFrame:
        if axis == 0: # drop rows (default)
            if type(indexer) in (pandas.DataFrame, pandas.Series):
                ix = indexer.index.intersection(self.stage.data.index)
            elif hasattr(indexer, '__len__') and not isinstance(indexer, str):
                try:
                    ix = pandas.Int64Index(indexer).intersection(
                        self.stage.data.index)
                except:
                    raise IndexerError(axis, indexer)
            else:
                raise IndexerError(axis, indexer)
                
            if len(ix) == 0:
                return self.proxy()

            layer = Layer()
            oper = op.DropRows(self.stage.data.loc[ix], self.stage.data.index)
            self.stage.data = layer.push(self.stage.data, oper)
            self.stage.add(layer)

            return self.proxy()

        elif axis == 1: # drop columns
            if type(indexer) == pandas.DataFrame:
                ix = indexer.columns.intersection(self.stage.data.columns)
            elif type(indexer) == pandas.Series:
                ix = pandas.Index([indexer.name]).intersection(
                    self.stage.data.columns)
            elif hasattr(indexer, '__len__') and not isinstance(indexer, str):
                try:
                    ix = pandas.Index(indexer).intersection(
                        self.stage.data.index)
                except:
                    raise IndexerError(axis, indexer)
            elif type(indexer) == str:
                ix = pandas.Index([indexer]).intersection(
                    self.stage.data.columns)
            else:
                raise IndexerError(axis, indexer)
            
            if len(ix) == 0:
                return self.proxy()

            layer = Layer()
            oper = op.DropColumns(self.stage.data.loc[:,ix], self.stage.data.columns)
            self.stage.data = layer.push(self.stage.data, oper)
            self.stage.add(layer)
            
            return self.proxy()
    
    def _ext_cols(self, data: PandasObject) -> Operation:
        if type(data) is pandas.Series:
            if data.name is None:
                raise AxisLabelError

            ext_rows = self.stage.data.index.intersection(data.index)
            ext_data = data.loc[ext_rows]
            if len(ext_data) != len(self.stage.data):
                raise InsertionError(self.stage.data, ext_data)

            oper = op.AddColumns(pandas.DataFrame(ext_data))
            return oper
        elif type(data) is pandas.DataFrame:
            ext_cols = data.columns.difference(self.stage.data.columns)
            ext_rows = self.stage.data.index.intersection(data.index)
            ext_data = data.loc[ext_rows, ext_cols]
            if ext_data.shape[0] != self.stage.data.shape[0]:
                raise InsertionError(self.stage.data, ext_data)

            oper = op.AddColumns(ext_data)
            return oper
    
    def _ext_rows(self, data: pandas.DataFrame, 
        ext_rows: RowIndex, ext_cols: ColIndex) -> Operation:
        # data & stage columns must be equal
        if (len(data.columns.intersection(
            self.stage.data.columns)) != len(self.stage.data.columns)):
            raise ExtensionError(0)

        ext_data = data.loc[ext_rows, self.stage.data.columns]
        oper = op.AddRows(ext_data)
    
    def _ext_both(self, data: pandas.DataFrame,
        ext_rows: RowIndex, ext_cols: ColIndex) -> Tuple[Operation, Operation]:
        # remove extension columns from extension rows to form row block
        row_block = data[:, ext_index]
        row_oper = self._ext_rows(row_block)
        col_block = data[ext_cols]
        col_oper = self._ext_cols(col_block)

        return (row_oper, col_oper)
        
    def extend(self, data: PandasObject) -> pandas.DataFrame:
        if type(data) is pandas.Series: # extend only cols
            layer = Layer()
            oper = self._ext_cols(data)
            self.stage.data = layer.push(self.stage.data, oper)
            self.stage.add(layer)
        elif type(data) is pandas.DataFrame:
            ext_cols = data.columns.difference(
                self.stage.data.columns)
            ext_rows = data.index.difference(
                self.stage.data.index)
            if len(ext_cols) == len(ext_rows) == 0:
                return self.proxy()
            elif len(ext_cols) == 0 and len(ext_rows) != 0: # extend only rows
                oper = self._ext_rows(data)
                layer = Layer()
                self.stage.data = layer.push(self.stage.data, oper)
                self.stage.add(layer)
            elif len(ext_cols) != 0 and len(ext_rows) == 0: # extend only cols
                oper = self._ext_cols(data)
                layer = Layer()
                self.stage.data = layer.push(self.stage.data, oper)
                self.stage.add(layer)
            elif len(ext_cols) != 0 and len(ext_rows) != 0: # extend rows & cols
                row_oper, col_oper = _ext_both(data, ext_rows, ext_cols)
                layer = Layer()
                queue = layer.push(self.stage.data, row_oper)
                self.stage.data = layer.push(queue, col_oper)
                self.stage.add(layer)
            else:
                raise ExtensionError(1)
        else:
            raise ObjectTypeError(data)

        return self.proxy()
    
    def set_index(self, data: Union[PandasObject, RowIndex]) -> pandas.DataFrame:
        if data.shape[0] != self.stage.data.shape[0]:
            raise SetIndexError(self.stage.data.shape[0], data.shape[0])
        layer = Layer()
        oper = op.Reindex(self.stage.data.index, data.index)
        self.stage.data = layer.push(self.stage.data, oper)
        self.stage.add(layer)

        return self.proxy()

    def commit(self):
        parent_id = self.head.id
        origin_hash = self.head.origin
        data_hash = hash_data(self.stage.data)
        delta = Delta(self)
        make = delta.make()
        node = Node(origin_hash, parent_id, make)
        node_str = node.to_json()
        node_id = hash_pair(hash_node(node_str), data_hash)
        
        node_path = os.path.join(self._tree.path, 'nodes', node_id)
        with open(node_path, 'w') as f:
            f.write(node_str)
        fs.write_delta(self._tree.path, node_id, make)

        arrow_path = os.path.join(self._tree.path, 'arrows', self.name)
        with open(arrow_path, 'w') as f:
            f.write(node_id)
        
        self.head = self._tree.node(node_id)
        self.stage.stack = []

        return repr(self)

    def _resolve_timeline(self) -> pandas.DataFrame:
        path = self._tree.path
        origin_name = self._origin[0]
        origin_id = self._origin[1]
        origin_hash = self.head.origin
        # load origin data and verify hash == origin_hash
        data = fs.load_origin(self._tree.path, origin_name)
        if hash_data(data) != origin_hash:
            raise IntegrityError(origin_name, 'origin')
        # apply deltas in timeline (excluding origin node)
        for node_id in list(self._timeline)[1:]:
            node_hash = self._timeline[node_id]
            for key, block in fs.iter_delta(path, node_id):
                if key in block_function:
                    data = block_function[key](data, block)
            
            # assure reconstructed node_id matches true node_id
            data_hash = hash_data(data)
            if hash_pair(node_hash, data_hash) != node_id:
                raise IntegrityError(node_id, 'delta')

        return data
    
    def __str__(self):
        out = "{0} -> {1}"
        return out.format(self.name, self.head.id)
    
    __repr__ = __str__



