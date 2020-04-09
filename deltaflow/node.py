import json
from typing import TypeVar, Union
from datetime import datetime
from collections import OrderedDict
from deltaflow.fs import delta_reader, read_meta, read_block
from deltaflow.hash import hash_node
from deltaflow.delta import block_order
from deltaflow.block import class_map

DataFrame = TypeVar('pandas.DataFrame')
PandasObject = TypeVar('PandasObject')

class Make:
    def __init__(self, path: str, node_id: str, make: list):
        self._path = path
        self._node_id = node_id
        self._make = make

    def read_block(self, i: int) -> PandasObject:
        upper = len(self._make)
        if i < 1 or i > upper:
            bounds = (1, upper)
            raise IndexError("node has blocks {0}-{1}".format(*bounds))
        
        meta, block = read_block(self._path, self._node_id, i - 1)
        key = meta['class']
        obj = class_map[key].unwrap(meta, block)

        return obj
    
    def _display(self, depth: int = 1):
        out = '[\n'
        just = ' ' * (depth * 2)
        i = 1
        for entry in self._make:
            out += "{0}{1}: {2}\n".format(just, i, entry)
            i += 1
        
        out += ' ' * ((2 * depth) - 2) + ']'
        return out
    
    def __str__(self):
        return self._display()

    __repr__ = __str__

class Node:
    private = ('_path', '_id', '_node', '_node_hash', '_node_type', 'make')
    def __init__(self, path: str, node_id: str, node_hash: str, node: OrderedDict):
        self.__dict__['_path'] = path
        self.__dict__['_id'] = node_id
        self.__dict__['_node'] = node
        self.__dict__['_node_hash'] = node_hash
        if node['parent'] is None:
            self.__dict__['_node_type'] = 'origin'
        else:
            self.__dict__['_node_type'] = 'node'
            self.__dict__['make'] = Make(path, node_id, node['make'])
    
    @property
    def id(self) -> str:
        return self._id
    
    def read_meta(self):
        deltafile = delta_reader(self._path, self._id)
        with deltafile as deltafile:
            meta = read_meta(deltafile)
        
        return meta
    
    def __getattr__(self, key):
        if key in self._node:
            return self._node[key]
        else:
            raise AttributeError
    
    def __setattr__(self, key, val):
        if key in NodeInfo.private:
            raise AttributeError
        else:
            self.__dict__[key] = val

    def __str__(self):
        just = '  '
        node = self._node
        out = "NODE[{0}]\n{{\n".format(self._id)
        out += "{0}origin: {1}\n".format(just, node['origin'])
        is_origin = node['parent'] is None
        parent = node['parent'] if not is_origin else 'null'
        out += "{0}parent: {1}\n".format(just, parent)
        out += "{0}shape: {1}\n".format(just, node['shape'])
        if not is_origin:
            out += "{0}make: {1}\n".format(
                just, self.make._display(depth=2))

        out += '}'
        return out

    __repr__ = __str__

transmap = {
    'drop_rows': lambda block: "DROP {0} ROW(S)".format(
        block.meta['shape'][0]),
    'drop_cols': lambda block: "DROP {0} COLUMN(S)".format(
        block.meta['shape'][0]),
    'reindex': lambda block: "REPLACE ROW INDEX",
    'rename': lambda block: "REPLACE COLUMN INDEX",
    'put': lambda block: "PUT {0} VALUES".format(block.meta['count']),
    'ext_rows': lambda block: "EXTEND ROWS BY {0}".format(
        block.meta['shape'][0]),
    'ext_cols': lambda block: "EXTEND COLUMNS BY {0}".format(
        block.meta['shape'][1])
}

def translate_make(make: OrderedDict) -> list:
    entry = []
    for key in make:
        if key in transmap:
            entry.append(transmap[key](make[key]))
        
    return entry

def make_node(origin_hash: str, parent_id: str, make: OrderedDict):
    node = [
        ('origin', origin_hash),
        ('parent', parent_id),
        ('shape', 
            (make['index'].meta['shape'][0], 
            make['columns'].meta['shape'][0])
        ),
        ('make', translate_make(make))
    ]

    node = OrderedDict(node)
    return json.dumps(node)

def make_origin(origin_hash: str, data: DataFrame) -> str:
    node = [
        ('origin', origin_hash),
        ('parent', None),
        ('shape', data.shape)
    ]

    node = OrderedDict(node)
    return json.dumps(node)