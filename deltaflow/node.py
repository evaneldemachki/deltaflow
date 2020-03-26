import json
from typing import TypeVar, Union
from datetime import datetime
from collections import OrderedDict
from deltaflow.hash import hash_node
from deltaflow.delta import block_order, block_function

DataFrame = TypeVar('pandas.DataFrame')

transmap = {
    'drop_rows': lambda item: "DROP {0} ROW(S)".format(
        len(item)),
    'drop_cols': lambda item: "DROP {0} COLUMNS(S)".format(
        len(item)),
    'reindex': lambda item: "ALTER ROW INDEX",
    'rename': lambda item: "ALTER COLUMN INDEX",
    'put_data': lambda item: "PUT {0} VALUE(S)".format(
        item.isna().sum().sum()),
    'ext_rows': lambda item: "APPEND {0} ROW(S)".format(
        item.shape[0]),
    'ext_cols': lambda item: "APPEND {0} COLUMN(S)".format(
        item.shape[1])
}

def translate_make(make: OrderedDict) -> list:
    entry = []
    for key in make:
        if make[key] is not False and key in transmap:
            entry.append(transmap[key](make[key]))
    
class Node(OrderedDict):
    def __init__(self, origin_hash: str, parent_id: Union[str, None], make: OrderedDict):
        node = [
            ('origin', origin_hash),
            ('parent', parent_id),
            ('shape', (len(make['index']), len(make['columns'])))
        ]
        super().__init__(node)
        self['make'] = translate_make(make)
    
    def to_json(self):
        return json.dumps(self)

class OriginNode(OrderedDict):
    def __init__(self, origin_hash: str, data: DataFrame):
        node = [
            ('origin', origin_hash),
            ('parent', None),
            ('shape', data.shape)
        ]
        super().__init__(node)
    
    def to_json(self):
        return json.dumps(self)



