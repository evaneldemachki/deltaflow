import os
import json
import pandas
from collections import OrderedDict
from deltaflow.errors import NameLookupError, IdLookupError
from deltaflow.hash import hash_node
from deltaflow.node import DeltaNode, OriginNode
from deltaflow.abstract import DirectoryMap

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

class ArrowsIndex(DirectoryMap):
    def __init__(self, path):
        super().__init__(
            os.path.join(path, 'arrows'),
            'arrows'
        )

    def _parse(self, text: str) -> str:
        return text

    def _show(self, key: str, obj: object) -> str:
        return "{0} -> {1}".format(key, obj)

class NodesIndex(DirectoryMap):
    def __init__(self, tree):
        super().__init__(
            os.path.join(tree.path, 'nodes'),
            'nodes'
        )
        self._tree = tree
        self._cache = {}
        self._cache = dict(self.items())

    def _parse(self, text: str) -> dict:
        return json.loads(text)

    def __getitem__(self, key: str) -> dict:
        if key in self._cache:
            return self._cache[key]
        else:
            try:
                return super().__getitem__(key)
            except KeyError:
                raise IdLookupError(key)
    
    def __str__(self):
        return self._tree.__str__


class Tree:
    def __init__(self, path):
        self.path = os.path.join(path, '.deltaflow')
        self.arrows = ArrowsIndex(self.path)
        self.nodes = NodesIndex(self)

    @property
    def origins(self):
        with open(os.path.join(self.path, 'origins'), 'r') as f:
            obj = json.load(f)
        
        return obj

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
    def node(self, node_id: str) -> DeltaNode:
        if node_id not in self.nodes:
            raise IdLookupError(node_id)
        
        node_dict = self.nodes[node_id]
        if node_dict['type'] == 'origin':
            node = OriginNode(self.path, node_id, node_dict)
        else:
            node = DeltaNode(self.path, node_id, node_dict)
            
        return node

    # get arrow node_id pointer by arrow name
    def arrow_head(self, name: str) -> str:
        path = os.path.join(self.path, 'arrows', name)  
        try:
            with open(path, 'r') as f:
                node_id = f.readline()
        except FileNotFoundError:
            raise NameLookupError('arrow', name)
        
        return node_id
    
    # return map of node lineage mapped to resp. node hashes
    def outline(self, node: DeltaNode) -> OrderedDict:
        path = os.path.join(self.path, 'nodes')
        outline = []
        node_str = json.dumps(node._node)
        outline.append((node.id, hash_node(node_str)))

        if node.type == 'origin':
            outline = OrderedDict(outline)
            return outline

        lineage = [node.id] + node.lineage
        for node_id in lineage[1:]:
            node_str = json.dumps(self.nodes[node_id])
            outline.append((node_id, hash_node(node_str)))

        outline = OrderedDict(reversed(outline))
        return outline

    def __str__(self):
        origins = self.origins
        origin_map = {node_id: name for name, node_id in origins.items()}

        node_links = {name: NodeLink(origins[name]) for name in origins}
        for _, node_id in self.arrows.items():
            node = self.nodes[node_id]
            if node['type'] == 'delta':
                lineage = [node_id] + node['lineage']
                origin_name = origin_map[lineage.pop(-1)]
                lineage = reversed(lineage)
            
                link = node_links[origin_name]
                for child in lineage:
                    link = link.add_child(child)

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


