import os
import pandas
from collections import OrderedDict
from typing import TypeVar, Union, Iterable, Any, Tuple
from deltaflow.errors import (
    UndoError, IndexerError, IntegrityError, 
    AxisLabelError, InsertionError, 
    ExtensionError, ObjectTypeError
)
from deltaflow import fs
from deltaflow.hash import hash_data, hash_pair, hash_node
from deltaflow.delta import Delta, block_function
from deltaflow.general import column_index, shrink
from deltaflow.node import Node
import deltaflow.operation as op

Tree = TypeVar('Tree')
Operation = op.Operation
PandasObject = Union[pandas.DataFrame, pandas.Series]
RowIndex = Union[pandas.RangeIndex, pandas.Int64Index]
ColIndex = pandas.Index
DataFrameIndexer = Union[PandasObject, RowIndex, ColIndex, Iterable, int, str]

class ArrowData:
    def __init__(self, data: pandas.DataFrame):
        self.base = data
        self.stage = data.copy()

class Layer:
    def __init__(self):
        self.batch = []
    
    def push(self, data: pandas.DataFrame, operation: Operation) -> pandas.DataFrame:
        self.batch.append(operation)
        queue = operation.execute(data)
        return queue

class Stack:
    def __init__(self):
        self.layers = []
    
    def add(self, layer: Layer) -> None:
        self.layers.append(layer)
    
    def revert(self, data: pandas.DataFrame) -> pandas.DataFrame:
        try:
            layer = self.layers[-1]
            for oper in reversed(layer.batch):
                data = oper.undo(data)
            
            self.layers.pop()
        except IndexError:
            raise UndoError

        return data
    
    # iterate through stack layers as flat sequence of operations
    def iter_operations(self):
        for layer in self.layers:
            for oper in layer.batch:
                yield oper

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

        self.data = ArrowData(self._resolve_timeline())
        self.stack = Stack()

    def proxy(self):
        return self.data.stage.copy()

    def undo(self):
        self.data.stage = self.stack.revert(self.data.stage)

    # update records in a segment of current stage dataset with
    # ... corresponding proxy records of matching indices
    def update(self, data: PandasObject):
        # determine matching column names
        update_cols = column_index(self.data.stage).intersection(column_index(data))
        # determine matching row indices
        data_index = pandas.Int64Index(data.index)
        stage_index = pandas.Int64Index(self.data.stage.index)
        update_rows = stage_index.intersection(data_index)
        # select update segments
        stage = pandas.DataFrame(
            self.data.stage.loc[update_rows, update_cols])
        data = pandas.DataFrame(
            data.loc[update_rows, update_cols])
        # shrink modifications
        x = shrink(data, stage)
        y = shrink(stage, data)
        layer = Layer()
        queue = layer.push(self.data.stage, op.Update(x, y))
        self.stack.add(layer)
  
    def drop(self, indexer: DataFrameIndexer, axis: int = 0) -> pandas.DataFrame:
        if axis == 0: # drop rows (default)
            if type(indexer) in (pandas.DataFrame, pandas.Series):
                ix = indexer.index.intersection(self.data.stage.index)
            elif hasattr(indexer, '__len__') and not isinstance(indexer, str):
                try:
                    ix = pandas.Int64Index(indexer).intersection(
                        self.data.stage.index)
                except:
                    raise IndexerError(axis, indexer)
            else:
                raise IndexerError(axis, indexer)
                
            if len(ix) == 0:
                return self.proxy()

            layer = Layer()
            oper = op.DropRows(self.data.stage.loc[ix], self.data.stage.index)
            self.data.stage = layer.push(self.data.stage, oper)
            self.stack.add(layer)

            return self.proxy()

        elif axis == 1: # drop columns
            if type(indexer) == pandas.DataFrame:
                ix = indexer.columns.intersection(self.data.stage.columns)
            elif type(indexer) == pandas.Series:
                ix = pandas.Index([indexer.name]).intersection(
                    self.data.stage.columns)
            elif hasattr(indexer, '__len__') and not isinstance(indexer, str):
                try:
                    ix = pandas.Index(indexer).intersection(
                        self.data.stage.index)
                except:
                    raise IndexerError(axis, indexer)
            elif type(indexer) == str:
                ix = pandas.Index([indexer]).intersection(
                    self.data.stage.columns)
            else:
                raise IndexerError(axis, indexer)
            
            if len(ix) == 0:
                return self.proxy()

            layer = Layer()
            oper = op.DropColumns(self.data.stage.loc[:,ix], self.data.stage.columns)
            self.data.stage = layer.push(self.data.stage, oper)
            self.stack.add(layer)
            
            return self.proxy()
    
    def _ext_cols(self, data: PandasObject) -> Operation:
        if type(data) is pandas.Series:
            if data.name is None:
                raise AxisLabelError
            ext_rows = self.data.stage.index.difference(
                data.index)
            ext_data = data[ext_rows]
            if len(ext_data) != len(self.data.stage):
                raise InsertionError(self.data.stage, ext_data)

            oper = op.AddColumns(pandas.DataFrame(ext_data))
            return oper
    
    def _ext_rows(self, data: pandas.DataFrame, 
        ext_rows: RowIndex, ext_cols: ColIndex) -> Operation:
        # data & stage columns must be equal
        if (len(data.columns.intersection(
            self.data.stage.columns)) != len(self.data.stage.columns)):
            raise ExtensionError(0)

        ext_data = data.loc[ext_rows, self.data.stage.columns]
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
            self.data.stage = layer.push(self.data.stage, oper)
            self.stack.add(layer)
        elif type(data) is pandas.DataFrame:
            ext_cols = data.columns.difference(
                self.data.stage.columns)
            ext_rows = data.index.difference(
                self.data.stage.index)
            if len(ext_cols) == len(ext_rows) == 0:
                return self.proxy()
            elif len(ext_cols) == 0 and len(ext_rows) != 0: # extend only rows
                oper = self._ext_rows(data)
                layer = Layer()
                self.data.stage = layer.push(self.data.stage, oper)
                self.stack.add(layer)
            elif len(ext_cols) != 0 and len(ext_rows) != 0: # extend rows & cols
                row_oper, col_oper = _ext_both(data, ext_rows, ext_cols)
                layer = Layer()
                queue = layer.push(self.data.stage, row_oper)
                self.data.stage = layer.push(queue, col_oper)
                self.stack.add(layer)
            else:
                raise ExtensionError(1)
        else:
            raise ObjectTypeError(data)

        return self.proxy()
    
    def set_index(self, data: Union[PandasObject, RowIndex]) -> pandas.DataFrame:
        if data.shape[0] != self.data.stage.shape[0]:
            raise SetIndexError(self.data.stage.shape[0], data.shape[0])
        layer = Layer()
        oper = op.Reindex(self.data.stage.index, data.index)
        self.data.stage = layer.push(self.data.stage, oper)
        self.stack.add(layer)

        return self.proxy()

    def commit(self):
        parent_id = self.head.id
        origin_hash = self.head.origin
        data_hash = hash_data(self.data.stage)
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



