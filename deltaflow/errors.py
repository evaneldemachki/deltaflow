import pandas

class Error(Exception):
    def __str__(self):
        return self.msg

strf_axis = {
    0: {
        'name': 'row',
        'base_types': ('pandas.DataFrame',),
        'indexer_types': (
            'pandas.Int64Index', 
            'pandas.RangeIndex',
            'list', 'tuple', 'int')
    },
    1: {
        'name': 'column',
        'base_types': ('pandas.DataFrame', 'pandas.Series'),
        'indexer_types': (
            'pandas.Index', 'list',
            'tuple', 'str')
    }
}

class IndexerError(Error):
    """raised on invalid row/column indexer type"""
    msg = "invalid {0} indexer: expected type(s) '{1}', got '{2}'"
    def __init__(self, axis, indexer):
        self.msg = self.msg.format(
            strf_axis[axis]['name'], 
            strf_axis[axis]['indexer_types'],
            type(indexer)
        )

class IntersectionError(Error):
    """called when passed index does not intersect with live data"""
    msg = "index does not intersect with live data index"
    def __init__(self):
        self.msg = self.msg

class DifferenceError(Error):
    """called when passed index is identical to live data"""
    msg = "index does not differ from live data index"
    def __init__(self):
        self.msg = self.msg
        
class ExtensionError(Error):
    """raised when extension indices do not match stage"""
    msg = "{0} of {1} extension must must match stage"
    strf = {
        0: ('columns', 'row'),
        1: ('rows', 'column')
    }
    def __init__(self, axis):
        self.msg = self.msg.format(*self.strf[axis])

class PutError(Error):
    """raised when DataFrame is identical to live data at intersection"""
    msg = "data is identical to live data at intersection"
    def __init__(self):
        self.msg = self.msg

class IntegrityError(Error):
    """raised when delta-node/origin-node hash pair does not match node ID"""
    msg = "{0}.{1} is corrupted or has been modified outside of DeltaFlow"
    def __init__(self, key, obj_type):
        self.msg = self.msg.format(key, obj_type)

class SetIndexError(Error):
    """raised on attempt to set index of incorrect length"""
    msg = "expected index of length '{0}', got '{1}'"
    def __init__(self, index_x, index_y):
        self.msg = self.msg.format(len(index_x), len(index_y))

class InsertionError(Error):
    """raised on attempt to insert columns of incorrect length"""
    msg = "expected data of length '{0}', got '{1}'"
    def __init__(self, index_x, index_y):
        self.msg = self.msg.format(len(index_x), len(index_y))

class FieldPathError(Error):
    """raised when '.deltaflow' directory not in path"""
    msg = "path '{0}' is not a DeltaFlow field directory"
    def __init__(self, path):
        self.msg = self.msg.format(path)

class NameExistsError(Error):
    msg = "{0} with name '{1}' already exists"
    def __init__(self, obj, name):
        self.msg = self.msg.format(obj, name)

class NameLookupError(Error):
    """raised when arrow/origin name not found in field"""
    msg = "{0} '{1}' not found in field"
    def __init__(self, obj, name):
        self.msg = self.msg.format(obj, name)

class InformationError(Error):
    """raised when origin/delta result in existing node ID"""
    msg = "{0} is identical to existing delta '{1}'"
    def __init__(self, obj, obj_hash):
        self.msg = self.msg.format(obj, obj_hash)

class IdLookupError(Error):
    """raised when node ID not found in field"""
    msg = "node with ID '{0}' not found"
    def __init__(self, node_id):
        self.msg = self.msg.format(node_id)
    
class ObjectTypeError(Error):
    """raised when pandas DataFrame or Series is expected"""
    msg = "invalid pandas object: expected type(s) '{0}', got '{1}'"
    def __init__(self, obj, axis=0):
        self.msg = self.msg.format(
            strf_axis[axis]['base_types'], type(obj))

class AxisLabelError(Error):
    """raised when column object with no axis labels is passed"""
    msg_dataframe = "'{0}' object requires {1}"
    msg_map = {
        'Series': ['pandas.Series', 'name attribute'],
        'DataFrame': ['pandas.DataFrame', 'column labels']
    }
    def __init__(self, obj_type):
        self.msg = self.msg.format(*self.msg_map[obj_type])


class AxisOverlapError(Error):
    """raised when column extension contains existing labels"""
    msg = "column extension contains existing labels"
    def __init__(self):
        self.msg = self.msg
    
class DataTypeError(Error):
    """raised when data types do not match stage"""
    msg = "data types must match stage at intersections"
    def __init__(self):
        self.msg = self.msg

class UndoError(Error):
    """raised when undo is called with empty stack"""
    msg = "nothing to undo"
    def __init__(self):
        self.msg = self.msg

class BlockError(Error):
    """raised on block apply method failure"""
    def __init__(self, msg):
        self.msg = msg
