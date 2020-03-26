class Error(Exception):
    def __str__(self):
        return self.msg

class FieldPathError(Error):
    msg = "path '{0}' does not contain field"
    def __init__(self, path):
        self.msg = FieldPathError.msg.format(path)

class OriginNameExistsError(Error):
    msg = "origin with name '{0}' already exists"
    def __init__(self, name):
        self.msg = OriginNameExistsError.msg.format(name)

class OriginNotFoundError(Error):
    msg = "origin with name '{0}' not found"
    def __init__(self, name):
        self.msg = OriginNotFoundError.msg.format(name)

class IdenticalOriginError(Error):
    msg = "attempting to add duplicate origin"
    def __init__(self):
        self.msg = IdenticalOriginError.msg

class NodeNotFoundError(Error):
    msg = "node with ID '{0}' not found"
    def __init__(self, node_id):
        self.msg = NodeNotFoundError.msg.format(node_id)

class InvalidRowIndexerError(Error):
    msg = "object of type '{0}' is not a valid row indexer"
    def __init__(self, objtype):
        self.msg = InvalidRowIndexerError.msg.format(objtype)

class InvalidColumnIndexerError(Error):
    msg = "object of type '{0}' is not a valid column indexer"
    def __init__(self, objtype):
        self.msg = InvalidColumnIndexerError.msg.format(objtype)
    
class NullIndexerError(Error):
    msg = "empty or non-intersecting indexer"
    def __init__(self):
        self.msg = NullIndexerError.msg

class InvalidPandasObjectError(Error):
    msg = "expected DataFrame or Series, got '{0}'"
    def __init__(self, objtype):
        self.msg = InvalidPandasObjectError.msg.format(objtype)

class UnnamedSeriesError(Error):
    msg = "cannot add unnamed Series as column"
    def __init__(self):
        self.msg = UnnamedSeriesError.msg

class SeriesNameError(Error):
    msg = "Series name '{0}' is already a column name"
    def __init__(self, name):
        self.msg = SeriesNameError.msg.format(name)

class SeriesLengthError(Error):
    msg = "Series length does not match stage data"
    def __init__(self):
        self.msg = SeriesLengthError.msg

class ExtensionIndexError(Error):
    msg = "extension must contain new row and/or column indices"
    def __init__(self):
        self.msg = ExtensionIndexError.msg

class RowExtensionError(Error):
    msg = "row extension block must contain all stage columns"
    def __init__(self):
        self.msg = RowExtensionError.msg

class ColExtensionError(Error):
    msg = "col extension block must contain all stage rows"
    def __init__(self):
        self.msg = ColExtensionError.msg
    
class TakeIndexError(Error):
    msg = "expected object of length '{0}', got '{1}'"
    def __init__(self, l1, l2):
        self.msg = TakeIndexError.msg.format(l1, l2)
    
class EmptyStackError(Error):
    msg = "Stack is empty"
    def __init__(self):
        self.msg = EmptyStackError.msg

class OriginIntegrityError(Error):
    msg = "origin has been modified outside of deltaflow"
    def __init__(self):
        self.msg = OriginIntegrityError.msg

class InvalidBlockError(Error):
    msg = "block key '{0}' not recognized"
    def __init__(self, key):
        self.msg = InvalidBlockError.msg.format(key)

class DeltaIntegrityError(Error):
    msg = "delta has been modified outside of deltaflow"
    def __init__(self):
        self.msg = DeltaIntegrityError.msg

class OriginFileNameError(Error):
    msg = "file '{0}' already exists in field directory"
    def __init__(self, name):
        self.msg = OriginFileNameError.msg.format(name)

class ArrowNameExistsError(Error):
    msg = "arrow with name '{0}' already exists"
    def __init__(self, name):
        self.msg = ArrowNameExistsError.msg.format(name)

class ArrowPointerError(Error):
    msg = "'{0}' is not a valid arrow pointer"
    def __init__(self, pointer):
        self.msg = ArrowPointerError.msg.format(pointer)

class ArrowNameError(Error):
    msg = "arrow with name '{0}' does not exist"
    def __init__(self, name):
        self.msg = ArrowNameError.msg.format(name)