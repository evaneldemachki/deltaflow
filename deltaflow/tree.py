import os
import json
import pandas
from collections import OrderedDict
from deltaflow.errors import *
from deltaflow.hash import hash_node

class NodeLink:
    def __init__(self, node_id: str):
        self.id = node_id
        self.children = []

    def add_child(self, child_id):
        if child_id not in self.children_ids:
            obj = NodeLink(child_id)
            self.children.append(obj)
            return obj
        else:
            return self.get_child(child_id)
    
    def get_child(self, child_id):
        i = self.children_ids.index(child_id)
        return self.children[i]

    @property
    def children_ids(self) -> list:
        return [obj.id for obj in self.children]

class NodeInfo:
    private = ('_id', '_node', '_node_hash')
    def __init__(self, node_id: str, node_hash: str, node: OrderedDict):
        self.__dict__['_id'] = node_id
        self.__dict__['_node'] = node
        self.__dict__['_node_hash'] = node_hash
        if node['parent'] is None:
            self.__dict__['_node_type'] = 'origin'
        else:
            self.__dict__['_node_type'] = 'node'
    
    @property
    def id(self) -> str:
        return self._id

    def __getattr__(self, key):
        if key == 'make':
            return display_make(self._node['make'])
        elif key in self._node:
            return self._node[key]
        else:
            raise AttributeError
    
    def __setattr__(self, key, val):
        if key in NodeInfo.private:
            raise AttributeError
        else:
            self.__dict__[key] = val

    def __str__(self):
        return display_node(self)

    __repr__ = __str__

class TreeIndex:
    private = ('_items')

    def to_dict(self):
        return self._items

    def __iter__(self):
        for key in self._items:
            yield key

    def __contains__(self, key):
        return key in self._items

    def __getitem__(self, key):
        return self._items[key]
    
    def __getattr__(self, key):
        if key in self._items:
            return self._items[key]
        else:
            raise AttributeError

    def __setattr__(self, key, val):
        if key in OriginsInfo.private:
            raise AttributeError
        else:
            self.__dict__[key] = val

class NodesIndex(TreeIndex):
    def __init__(self, nodes):
        self.__dict__['_items'] = nodes
    
    def __str__(self):
        out = 'NODES: {\n'
        for key in self:
            out += "  {1}: {1}\n".format(key, self[key])
        
        out += "}"
        return out

class OriginsIndex(TreeIndex):
    def __init__(self, origins):
        self.__dict__['_items'] = origins

    def __str__(self):
        out = 'ORIGINS: {\n'
        for key in self:
            out += "  {0}: {1}\n".format(key, self[key])
        
        out += "}"
        return out
    
    __repr__ = __str__

class ArrowsIndex(TreeIndex):
    def __init__(self, arrows):
        self.__dict__['_items'] = arrows

    def __str__(self):
        out = 'ARROWS: {\n'
        for key in self:
            out += "  {0} -> {1}\n".format(key, self._items[key])
        
        out += "}"
        return out
    
    __repr__ = __str__


class Tree:
    def __init__(self, path):
        self.__dict__['path'] = os.path.join(path, '.deltaflow')

    @property
    def origins(self):
        with open(os.path.join(self.path, 'origins'), 'r') as f:
            obj = json.load(f)
        
        return OriginsIndex(obj)
    
    @property
    def arrows(self):
        path = os.path.join(self.path, 'arrows')
        arrows_list = os.listdir(path)
        obj = {}
        for name in arrows_list:
            with open(os.path.join(path, name), 'r') as f:
                obj[name] = f.readline()

        return ArrowsIndex(obj)
    
    @property
    def nodes(self):
        path = os.path.join(self.path, 'nodes')
        nodes_list = os.listdir(path)
        obj = {}
        for node_id in nodes_list:
            with open(os.path.join(path, node_id), 'r') as f:
                obj[node_id] = json.load(f)['parent']

        return NodesIndex(obj)        

    # get origin name from given origin id
    def name_origin(self, origin_id: str) -> str:
        name = None
        for key in self.origins:
            if self.origins[key] == origin_id:
                name = key
        if name is None:
            raise OriginNotFoundError

        return name

    # return NodeInfo object for a given node_id
    def node(self, node_id: str) -> NodeInfo:
        path = os.path.join(self.path, 'nodes', node_id)
        if not os.path.isfile(path):
            raise NodeNotFoundError(node_id)

        with open(path, 'r') as f:
            node_str = f.read()
        node_hash = hash_node(node_str)

        nodeinfo = NodeInfo(
            node_id, node_hash,
            json.loads(node_str, object_pairs_hook=OrderedDict)
        ) 
        return nodeinfo

    # get arrow node_id pointer by arrow name
    def arrow_head(self, name: str) -> str:
        path = os.path.join(self.path, 'arrows', name)  
        try:
            with open(path, 'r') as f:
                pointer = f.readline()
        except FileNotFoundError:
            raise ArrowNameError(name)
        
        return pointer
    
    # get timeline of parent node_ids mapped to resp. node_hashes
    def timeline(self, node_id: str) -> OrderedDict:
        timeline = list()
        path = os.path.join(self.path, 'nodes')
        node_path = os.path.join(path, node_id)
        try:
            with open(node_path, 'r') as f:
                node_str = f.read()
        except FileNotFoundError:
            raise NodeNotFoundError(node_id)

        parent = json.loads(node_str)['parent']
        timeline.append((node_id, hash_node(node_str)))
        while parent is not None:
            node_id = parent
            node_path = os.path.join(path, node_id)
            try:
                with open(node_path, 'r') as f:
                    node_str = f.read()
            except FileNotFoundError:
                raise NodeNotFoundError(node_id)

            parent = json.loads(node_str)['parent']
            timeline.append((node_id, hash_node(node_str)))

        timeline = OrderedDict(reversed(timeline))
        return timeline
    
    def __setattr__(self, key, val):
        raise AttributeError

    def __str__(self):
        node_links = {}
        rev = {}
        origins = self.origins
        for name in origins:
            origin_id = origins[name]
            node_links[origin_id] = []
            rev[origin_id] = name
        
        arrows = self.arrows
        for name in arrows:
            history = []
            node_id = arrows[name]
            node = None
            while node_id is not None:
                history.append(node_id)
                path = os.path.join(self.path, 'nodes', node_id)
                with open(path, 'r') as f:
                    node = json.load(f)
                    node_id = node['parent']

            node_links[history[-1]].append(history)
        
        node_tree = []
        for origin_id in node_links:
            origin_obj = NodeLink(origin_id)
            for hist in node_links[origin_id]:
                obj = origin_obj
                for node_id in reversed(history[1:]):
                    obj = obj.add_child(node_id)

            node_tree.append(origin_obj)
        
        out = ''
        for obj in node_tree:
            out += expand_tree(obj)
        
        return out[:-1]
    
    __repr__ = __str__

def expand_tree(node, _prefix="", _last=True):
    out = _prefix + "|- " + node.id + '\n'
    _prefix += "|  " if _last else "|  "
    child_count = len(node.children)
    for i, child in enumerate(node.children):
        _last = i == (child_count - 1)
        out += expand_tree(child, _prefix, _last)
    
    return out

def display_make(make: list, depth: int = 1) -> str:
    out = '['
    just = ' ' * (depth * 2)
    for entry in make:
        out.append("{0}{1}\n".format(just, entry))
    
    out += ' ' * ((2 * depth) - 2) + ']'
    return out

def display_node(nodeinfo: NodeInfo) -> str:
    just = '  '
    out = "NODE[{0}]\n{\n".format(nodeinfo.id)
    out += "{0}origin: {1}\n".format(just, nodeinfo.parent)
    out += "{0}parent: {1}\n".format(just, nodeinfo.parent)
    out += "{0}timestamp: {1}\n".format(just, nodeinfo.timestamp)
    out += "{0}shape: {1}\n".format(just, nodeinfo.shape)
    out += "{0}make: {1}\n".format(
        just, display_make(nodeinfo._node['make'], depth=2))

    out += '}'
    return out


