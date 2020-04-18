import os
import pandas
import json
import hashlib
from datetime import datetime
from deltaflow.errors import (FieldPathError, NameExistsError, 
    InformationError, IdLookupError)
from deltaflow import fs
from deltaflow.hash import hash_data, hash_node
from deltaflow.tree import Tree
from deltaflow.arrow import Arrow
from deltaflow.node import make_origin

# Initialize a field directory in given path
def touch(path: str = os.getcwd()) -> None:
    core_path = os.path.join(path, '.deltaflow')
    try:
        os.mkdir(core_path)
    except FileExistsError:
        print("WARNING: new field not created (field already exists)")
        return
    except:
        raise

    orig_path = os.path.join(core_path, 'origins')
    with open(orig_path, 'w') as f:
        json.dump({}, f)

    os.mkdir(os.path.join(core_path, 'arrows'))
    os.mkdir(os.path.join(core_path, 'deltas'))
    os.mkdir(os.path.join(core_path, 'nodes'))

class Field:
    immutable = ('path', 'tree')
    def __init__(self, path: str = os.getcwd()):
        core_path = os.path.join(path, '.deltaflow')
        if not os.path.isdir(core_path):
            raise FieldPathError(path)

        self.__dict__['path'] = path
        self.__dict__['tree'] = Tree(path)
    
    # load field Arrow instance
    def arrow(self, name: str) -> Arrow:
        arrow = Arrow(self.tree, name)
        return arrow
        
    # add pandas dataframe as new origin with given name
    def add_origin(self, data: pandas.DataFrame, name: str) -> None:
        # create origin
        origin_hash = hash_data(data)
        node_str = make_origin(origin_hash, data)
        node_id = hash_node(node_str)

        origins = self.tree.origins
        if name in origins:
            raise NameExistsError('origin', name)
        for key in origins:
            if origins[key] == node_id:
                raise InformationError(key)
        
        fs.write_origin(self.path, name, data)

        path = os.path.join(self.tree.path, 'nodes', node_id)
        with open(path, 'w') as f:
            f.write(node_str)

        origins = origins
        origins[name] = node_id
        with open(os.path.join(self.tree.path, 'origins'), 'w') as f:
            json.dump(origins, f)
        
        arrow_path = os.path.join(self.tree.path, 'arrows', '.' + name)
        with open(arrow_path, 'w') as f:
            f.write(node_id)

    def add_arrow(self, node_id: str, name: str) -> None:
        if name in self.tree.arrows:
            raise NameExistsError('arrow', name)

        if node_id not in self.tree.nodes:
            raise IdLookupError(node_id)

        if name[0] == '.':
            raise NameError("'.' prefix is reserved for master arrows")

        arrow_path = os.path.join(self.tree.path, 'arrows', name)
        with open(arrow_path, 'w') as f:
            f.write(node_id)

    def __setattr__(self, key, val):
        if key in Field.immutable:
            raise AttributeError
        else:
            self.__dict__[key] = val

    def __str__(self):
        out = "deltaflow.Field('{0}')".format(self.path)
        return out

    __repr__ = __str__