import pandas
import numpy
from typing import TypeVar, Union
from collections import OrderedDict
from deltaflow import operation as op
from deltaflow.hash import hash_data
from deltaflow.block import IndexBlock, PutBlock, ExtensionBlock

PandasRowIndex = Union[pandas.Int64Index, pandas.RangeIndex]
Arrow = TypeVar('Arrow')
Stage = TypeVar('Stage')

# order that blocks must be read and written
block_order = ( # this order cannot be changed
    'drop_rows', 'drop_cols', 'reindex', 
    'rename', 'put', 'set_types',
    'ext_cols', 'ext_rows', 'index', 'columns'
)

class DeltaBase:
    def __init__(self, base):
        self.base = base
        self.data = base.copy()
        self.put_data = None
        self.set_types = None
        # track row drops
        self.dropped_rows = pandas.Int64Index([])
        # track row drops
        self.dropped_cols = pandas.Index([])
    
    def put(self, data:pandas.DataFrame) -> None:
        self.data.update(data)

    def rename_columns(self, cols_x: pandas.Index, cols_y: pandas.Index) -> None:
        ix = self.cmapix(cols_x)
        cols_x, cols_y = cols_x[ix], cols_y[ix]

        indexer = self.data.columns.get_indexer(cols_x)
        basecolumns = self.data.columns.to_numpy()
        numpy.put(basecolumns, indexer, cols_y)

        self.data.columns = pandas.Index(basecolumns)

    def reindex(self, index_x: PandasRowIndex, index_y: PandasRowIndex) -> None:
        ix = self.imapix(index_x)
        index_x, index_y = index_x[ix], index_y[ix]
        index_y = index_y.to_numpy()
        indexer = self.data.index.get_indexer(index_x)
        index = self.data.index.to_numpy()
        numpy.put(index, indexer, index_y)

        self.data.index = index

    def drop_rows(self, index: PandasRowIndex) -> None:
        index = self.imap(index)
        indexer = self.data.index.get_indexer(index)
        baseindex = self.base.index[indexer]
        
        self.data = self.data.drop(index)
        self.base = self.base.drop(baseindex)
        self.dropped_rows = self.dropped_rows.append(baseindex)

    def drop_cols(self, columns: pandas.Index) -> None:
        columns = self.cmap(columns)
        indexer = self.data.index.get_indexer(columns)
        basecolumns = self.base.columns[indexer]

        self.data = self.data.drop(columns, axis=1)
        self.base = self.base.drop(basecolumns, axis=1)
        self.dropped_cols = self.dropped_cols.append(basecolumns)
    
    # Return indexer of row indices that are in data
    def imapix(self, index: PandasRowIndex) -> numpy.array:
        indexer = self.data.index.get_indexer(index)
        ix = numpy.where(indexer!=-1)

        return ix

    # Return indexer of column names that are in data
    def cmapix(self, columns: pandas.Index) -> numpy.array:
        indexer = self.data.columns.get_indexer(columns)
        ix = numpy.where(indexer!=-1)

        return ix      

    # Return row indices that are in data
    def imap(self, index: PandasRowIndex) -> pandas.Int64Index:
        indexer = self.data.index.get_indexer(index)
        indexer = numpy.delete(indexer, numpy.where(indexer==-1))

        indexmap = self.data.index[indexer]
        return indexmap

    # Return column names that are in data
    def cmap(self, columns: pandas.Index) -> pandas.Index:
        indexer = self.data.columns.get_indexer(columns)
        indexer = numpy.delete(indexer, numpy.where(indexer==-1))

        columnmap = self.data.columns[indexer]
        return columnmap


class DeltaExtension:
    def __init__(self, stage: pandas.DataFrame):
        self.data = stage
        self.row_block = None
        self.col_block = None
        
class Delta:
    def __init__(self, arrow: Arrow):
        self.base = DeltaBase(arrow.stage.base)
        self.extension = DeltaExtension(arrow.stage.data)
        self.index = arrow.stage.data.index
        self.columns = arrow.stage.data.columns
        self.build(arrow.stage)
    
    def make(self) -> OrderedDict:
        base = self.base
        ext = self.extension

        scheme = OrderedDict()

        base_drop = [
            ('drop_rows', base.dropped_rows), 
            ('drop_cols', base.dropped_cols)
        ]
        for axis in range(len(base_drop)):
            key, obj = base_drop[axis]
            if obj.shape[0] > 0:
                scheme[key] = IndexBlock(obj, axis=axis, method='drop')
        
        base_set = [
            ('reindex', base.data.index, base.base.index),
            ('rename', base.data.columns, base.base.columns)
        ]
        for axis in range(len(base_set)):
            key, obj, comp = base_set[axis]
            if not (obj == comp).all():
                scheme[key] = IndexBlock(obj, axis=axis, method='set')
        
        if base.put_data is not None:
            scheme['put'] = PutBlock(base.put_data, dtypes=base.set_types)
        
        ext_blocks = [
            ('ext_cols', ext.col_block),
            ('ext_rows', ext.row_block)
        ]
        for i in range(len(ext_blocks)):
            axis = abs(i - 1)
            key, obj = ext_blocks[i]
            if obj is not None:
                scheme[key] = ExtensionBlock(obj, axis=axis)
        
        scheme['index'] = IndexBlock(self.index, axis=0, method='set')
        scheme['columns'] = IndexBlock(self.columns, axis=1, method='set')
        
        return scheme

    def build(self, stage: Stage) -> None:
        # stage 1: track base schema changes
        for oper in stage.iter_operations():
            optype = type(oper)
            if optype == op.DropRows:
                self.base.drop_rows(oper.rows.index)
            elif optype == op.DropColumns:
                self.base.drop_cols(oper.columns.columns)
            elif optype == op.Reindex:
                self.base.reindex(oper.index_x, oper.index_y)
            elif optype == op.RenameColumns:
                self.base.rename_columns(oper.cols_x, oper.cols_y)
            elif optype == op.Put:
                self.base.put(oper.data_y)
            else:
                continue
        # stage 2: calculate and shrink base modifications
        x = self.base.base
        y = self.base.data
        # match x & y index in preparation for shrink
        x.index = y.index
        x.columns = y.columns
        shrink_base = op.shrink(x, y)
        if not shrink_base.shape[0] == 0:
            self.base.put_data = shrink_base
            # dtype preservation
            cond = (x.dtypes.to_numpy() != shrink_base.dtypes.to_numpy())
            if type(cond) is numpy.array:
                cond = cond.any()
            if cond: # dtypes differ after shrink -> write dtypes to delta
                self.base.set_types = x.dtypes   
        # stage 3: calculate extension row, column block pair
        ext = self.extension
        ext_index = ext.data.index.difference(self.base.data.index)
        ext_cols = ext.data.columns.difference(self.base.data.columns)
        if len(ext_cols) != 0:
            ext.col_block = ext.data.loc[self.base.data.index, ext_cols]
        if len(ext_index) != 0:
            ext.row_block = ext.data.loc[ext_index, self.base.data.columns]
                








