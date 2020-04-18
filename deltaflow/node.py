import json
import pandas
from typing import TypeVar, Union, Tuple
from collections import OrderedDict
import deltaflow.fs as fs
from deltaflow.block import get_block
from deltaflow.abstract import Selection

DataFrame = pandas.DataFrame
PandasObject = TypeVar('PandasObject')

class Node:
    static = ['_path', '_id', '_node']
    def __init__(self, path: str, node_id: str, node: OrderedDict):
        self._path = path
        self._id = node_id
        self._node = node

    @property
    def id(self) -> str:
        return self._id
    
    def __getattr__(self, key):
        if key in self._node:
            return self._node[key]
        else:
            raise AttributeError
    
    def __setattr__(self, key, val):
        if key in self.static and key in self.__dict__:
            msg = "attribute '{0}' is read-only"
            raise AttributeError(msg.format(key))
        else:
            self.__dict__[key] = val
          
class DeltaNode(Node):
    static = Node.static + ['delta']
    def __init__(self, path: str, node_id: str, node: dict):
        super().__init__(path, node_id, node)
        self.delta = DeltaPointer(fs.DeltaFile(path, node_id))

    def __str__(self):
        node = self._node
        out = "NODE[{0}]: {{\n".format(self._id)
        out += "  type: {0}\n".format(node['type'])
        out += "  origin: {0}\n".format(node['origin'])
        lineage = "[{0}, ...] ({1})".format(
            node['lineage'][0], len(node['lineage']))
        out += "  lineage: {0}\n".format(lineage)

        out += '}'
        return out

    __repr__ = __str__


class OriginNode(Node):
    def __init__(self, path: str, node_id: str, node: OrderedDict):
        super().__init__(path, node_id, node)

    @property
    def id(self) -> str:
        return self._id

    def __str__(self):
        just = '  '
        node = self._node
        out = "NODE[{0}]\n{{\n".format(self._id)
        out += just + "type: {0}".format(node['type'])
        out += just + "origin: {1}\n".format(node['origin'])

        out += '}'
        return out

    __repr__ = __str__

class DeltaPointer(Selection):
    def __init__(self, delta_file: fs.DeltaFile):
        self._file = delta_file
        self._meta = delta_file.meta

        values = [self._meta[key] for key in self._meta]
    
        super().__init__('blocks', values)
    
    def _get(self, i: int) -> Tuple:
        obj = self._file.read_block(i)
        return obj
    
    def _show(self, i: int) -> Union[str, None]:
        entry = self._meta[list(self._meta)[i]]
        block = get_block(entry['class'])
        block_strings = block.stringify(entry)
        if len(block_strings) == 1:
            return block_strings[0]
        else:
            return "[{0} | {1}]".format(*block_strings)
    
def make_node(origin_hash: str, lineage: Tuple[str]):
    node = [
        ('type', 'delta'),
        ('origin', origin_hash),
        ('lineage', lineage)
    ]

    node = OrderedDict(node)
    return json.dumps(node)

def make_origin(origin_hash: str, data: DataFrame) -> str:
    node = [
        ('type', 'origin'),
        ('origin', origin_hash)
    ]

    node = OrderedDict(node)
    return json.dumps(node)