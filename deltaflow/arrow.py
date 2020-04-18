import os
import pandas
from collections import OrderedDict
from typing import TypeVar, Union, Iterable, Any, Tuple
import deltaflow.fs as fs
import deltaflow.operation as op
from deltaflow.hash import hash_data, hash_pair, hash_node
from deltaflow.delta import build
from deltaflow.node import make_node
from deltaflow.errors import (
    UndoError, IndexerError, IntegrityError, 
    AxisLabelError, InsertionError, 
    ExtensionError, ObjectTypeError,
    AxisOverlapError, DataTypeError, 
    DifferenceError, IntersectionError,
    PutError
)

DataFrame = pandas.DataFrame
Series = pandas.Series
Index = pandas.Index

PandasObject = Union[DataFrame, Series]
IndexLike = Union[PandasObject, Iterable]

class Layer:
    def __init__(self):
        self.batch = []
    
    def push(self, data: DataFrame, oper: 'Operation') -> DataFrame:
        self.batch.append(oper)
        queue = oper.execute(data)
        return queue

class Stage:
    def __init__(self, data: DataFrame):
        self.base = data
        self.live = data.copy()
        self.stack = []

    def add(self, layer: Layer) -> None:
        self.stack.append(layer)
    
    def revert(self) -> None:
        data = self.live.copy()
        try:
            layer = self.stack[-1]
            for oper in reversed(layer.batch):
                data = oper.undo(data)
            
            self.stack.pop()
        except IndexError:
            raise UndoError

        self.live = data
    
    # iterate through stack layers as flat sequence of operations
    def iter_operations(self):
        for layer in self.stack:
            for oper in layer.batch:
                yield oper
    
    def __str__(self):
        out = '[\n'
        for layer in self.stack:
            if len(layer.batch) == 1:
                out += '  ' + str(layer.batch[0]) + ',\n'
            else:
                out += '  [\n'
                for oper in layer.batch:
                    out += '    ' + str(oper) + ',\n'
                
                out += '  ]'
            
        out += ']'
        return out
    
    __repr__ = __str__

        
class Arrow:
    def __init__(self, tree: 'Tree', name: str):
        node_id = tree.arrow_head(name)
        self.name = name
        self.head = tree.node(node_id)
        self._tree = tree

        outline = tree.outline(self.head)
        self.stage = Stage(self._resolve(outline))

    def proxy(self) -> DataFrame:
        return self.stage.live.copy()

    def undo(self) -> DataFrame:
        try:
            self.stage.revert()
        except UndoError:
            print("WARNING: nothing un-done (stage is empty)")
            return None

        return self.proxy()

    # take difference between stage and proxy at shared indices, put difference in stage
    def put(self, data: PandasObject) -> DataFrame:
        if type(data) is Series:
            data = DataFrame(data)
        # determine columns that intersect with stage
        stage_columns = self.stage.live.columns
        update_cols = stage_columns.intersection(op.effective_axis(data, axis=1))
        # determine row indices that intersect with stage
        stage_index = self.stage.live.index
        update_rows = stage_index.intersection(data.index)
        # select update segments
        stage_select = self.stage.live.loc[update_rows, update_cols]
        data_select = data.loc[update_rows, update_cols]
        # data type preservation
        dt_pres = data_select.dtypes
        # shrink modifications in both directions
        x = op.shrink(data_select, stage_select)
        y = op.shrink(stage_select, data_select)
        # if layer is empty, return proxy as is
        if y.shape[0] == 0:
            return self.proxy()
            
        layer = Layer()
        self.stage.live = layer.push(self.stage.live, 
            op.Put(x, y, dt_pres))

        self.stage.add(layer)
        return self.proxy()
  
    def drop(self, index: IndexLike, axis: int = 0, method: str = 'intersection') -> DataFrame:
        if axis not in (0, 1):
            err_msg = "axis must be 0 or 1, got {0}"
            raise TypeError(err_msg.format(axis))

        if method == 'intersection': # (default) drop shared indices
            ix_method = Index.intersection
        elif method == 'difference': # drop rows that are not in stage
            ix_method = Index.difference
        else:
            raise ValueError("drop methods: ['intersection', 'difference']")

        ix_type = type(index)
        if ix_type in (DataFrame, Series):
            if ix_type is Series and axis == 1:
                if index.name is None:
                    raise AxisLabelError('Series')

            ix = ix_method(self.stage.live._get_axis(axis), 
                op.effective_axis(index, axis=axis))
        elif ix_type in (str, int):
            ix = ix_method(self.stage.live._get_axis(axis),
                pandas.Index([index]))

        if ix.shape[0] == 0:
            if method == 'intersection':
                raise IntersectionError
            else:
                raise DifferenceError

        layer = Layer()
        drop_slice = [slice(None), slice(None)]
        drop_slice[axis] = ix
        drop_data = self.stage.live.loc[drop_slice[0], drop_slice[1]]
        self.stage.live = layer.push(
            self.stage.live,
            op.Drop(
                drop_data, 
                self.stage.live._get_axis(axis), 
                axis
            )
        )
        self.stage.add(layer)
        return self.proxy()
        
    def extend(self, data: PandasObject, axis: int = 0) -> DataFrame:
        if axis == 0: # extend rows
            if type(data) is not DataFrame:
                err_msg = "expected DataFrame object, got '{0}'"
                raise TypeError(err_msg.format(type(data)))
            
            ext_rows = data.index.difference(self.stage.live.index)
            if ext_rows.shape[0] == 0:
                raise DifferenceError

            col_match = data.columns.intersection(self.stage.live.columns)
            if col_match.shape[0] != self.stage.live.shape[1]:
                raise ExtensionError(0)

            ext = data[col_match, ext_rows]
        elif axis == 1: # extend columns
            data_type = type(data)
            if data_type not in (DataFrame, Series):
                err_msg = "expected DataFrame/Series object, got '{0}'"
                raise TypeError(err_msg.format(data_type))
            if data_type is Series:
                if data.name is None:
                    raise AxisLabelError('Series')
            
            ext_cols = op.effective_axis(data, axis=1)
            ext_cols = ext_cols.difference(self.stage.live.columns)
            if ext_cols.shape[0] == 0:
                raise DifferenceError

            row_match = data.index.intersection(self.stage.live.index)
            if row_match.shape[0] != self.stage.live.shape[0]:
                raise ExtensionError(1)

            if data_type is Series:
                ext = pandas.DataFrame(data[row_match])
            else:
                ext = data.loc[row_match, ext_cols]
        else:
            err_msg = "axis must be 0 or 1, got {0}"
            raise TypeError(err_msg.format(axis))
            
        layer = Layer()
        self.stage.live = layer.push(
            self.stage.live,
            op.Extend(ext, axis=axis)
        )
        self.stage.add(layer)

        return self.proxy()
    
    def relabel(self, data: IndexLike, axis: int = 0) -> DataFrame:
        if data.shape[0] != self.stage.live.shape[0]:
            raise SetIndexError(self.stage.live.shape[0], data.shape[0])

        if data.shape[axis] != self.stage.live.shape[axis]:
            raise IndexError('axis length does not match live data')

        layer = Layer()
        self.stage.live = layer.push(
            self.stage.live, 
            op.Relabel(self.stage.live.index, data.index, axis=axis)
        )
        self.stage.add(layer)

        return self.proxy()

    def commit(self) -> None:
        if self.head.type == 'delta':
            lineage = [self.head.id] + self.head.lineage
        else:
            lineage = [self.head.id]

        origin_hash = self.head.origin
        data_hash = hash_data(self.stage.live)

        delta = build(self.stage)

        node_str = make_node(origin_hash, lineage)
        node_id = hash_pair(hash_node(node_str), data_hash)

        node_path = os.path.join(self._tree.path, 'nodes', node_id)
        with open(node_path, 'w') as f:
            f.write(node_str)

        fs.write_delta(self._tree.path, node_id, delta)

        arrow_path = os.path.join(self._tree.path, 'arrows', self.name)
        with open(arrow_path, 'w') as f:
            f.write(node_id)
        
        self.head = self._tree.node(node_id)
        self.stage.stack = []

        print(self)

    def _resolve(self, outline) -> DataFrame:
        path = self._tree.path

        if self.head.type == 'delta':
            origin_id = self.head.lineage[-1]
        else:
            origin_id = self.head.id

        origin_name = self._tree.name_origin(origin_id)
        origin_hash = self.head.origin
        # load origin data and verify hash == origin_hash
        data = fs.load_origin(self._tree.path, origin_name)
        if hash_data(data) != origin_hash:
            raise IntegrityError(origin_name, 'origin')
        # apply deltas in timeline (excluding origin node)
        for node_id in list(outline)[1:]:
            node_hash = outline[node_id]
            delta_file = fs.DeltaFile(path, node_id)
            for modifier in delta_file.iter_blocks():
                data = modifier(data)
            
            # assure reconstructed node_id matches true node_id
            data_hash = hash_data(data)
            if hash_pair(node_hash, data_hash) != node_id:
                raise IntegrityError(node_id, 'delta')

        return data
    
    def __str__(self):
        out = "{0} -> {1}"
        return out.format(self.name, self.head.id)
    
    __repr__ = __str__