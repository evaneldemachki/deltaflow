import os
import json
import pandas
from collections import OrderedDict
from deltaflow.errors import NameLookupError, IdLookupError
from deltaflow.hash import hash_node
from deltaflow.node import Node

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

    def __getattr__(self, key):
        if key in self._items:
            return self._items[key]
        else:
            raise AttributeError

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
            raise KeyError('origin not found')

        return name

    # return Node object for a given node_id
    def node(self, node_id: str) -> Node:
        node_path = os.path.join(self.path, 'nodes', node_id)
        if not os.path.isfile(node_path):
            raise IdLookupError(node_id)

        with open(node_path, 'r') as f:
            node_str = f.read()
        node_hash = hash_node(node_str)

        nodeinfo = Node(
            self.path, node_id, node_hash,
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
            raise NameLookupError('arrow', name)
        
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
            raise IdLookupError(node_id)

        parent = json.loads(node_str)['parent']
        timeline.append((node_id, hash_node(node_str)))
        while parent is not None:
            node_id = parent
            node_path = os.path.join(path, node_id)
            try:
                with open(node_path, 'r') as f:
                    node_str = f.read()
            except FileNotFoundError:
                raise IdLookupError(node_id)

            parent = json.loads(node_str)['parent']
            timeline.append((node_id, hash_node(node_str)))

        timeline = OrderedDict(reversed(timeline))
        return timeline
    
    def __setattr__(self, key, val):
        raise AttributeError

    def __str__(self):
        origins = self.origins.to_dict()
        origin_map = {node_id: name for name, node_id in origins.items()}

        node_links = {name: NodeLink(origins[name]) for name in origins}
        nodes = self.nodes
        for items in self.arrows.to_dict().items():
            node_id = items[1]
            timeline = []
            parent = node_id
            while parent is not None:
                timeline.append(parent)
                parent = nodes[parent]
                    
            origin_name = origin_map[timeline.pop(-1)]
            timeline = reversed(timeline)
            
            node = node_links[origin_name]
            for child in timeline:
                node = node.add_child(child)

        out = ''
        for name in node_links:
            out += name + '\n'
            out += expand_tree(node_links[name]) + '\n'

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


