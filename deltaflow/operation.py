import pandas
import numpy

# return column index for pandas object
def column_index(obj):
    if isinstance(obj, pandas.Series):
        cols = pandas.Index([obj.name])
    elif isinstance(obj, pandas.DataFrame):
        cols = pandas.Index(obj.columns)
    else: raise TypeError

    return cols

# return reduced dataframe of modified rows from x to y
def shrink(x, y):
    diff = y[~y.isin(x)]
    diff = diff.dropna(axis='columns', how='all')
    diff = diff.dropna(axis='rows', how='all')

    return diff

class Put:
    __slots__ = ['id', 'data_x', 'data_y', 'dtypes']
    def __init__(self, data_x, data_y, dtypes):
        self.id = 'put'
        self.data_x = data_x
        self.data_y = data_y
        self.dtypes = dtypes

    def execute(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data.update(self.data_y)
        data = data.astype(self.dtypes[self.data_y.columns])
        return data
    
    def undo(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data.update(self.data_x)
        data = data.astype(self.dtypes[self.data_x.columns])

        return data
    
    def __str__(self):
        total = self.data_y.count().sum()
        return "PUT {0} VALUES".format(total)

    def __repr__(self):
        return repr(self.data_y)

# Add rows to the end of dataframe
class ExtendRows:
    __slots__ = ['id', 'rows']
    def __init__(self, rows: pandas.DataFrame):
        self.id = 'ext_rows'
        self.rows = rows
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.append(self.rows)
        return data
    
    def undo(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.drop(self.rows.index)
        return data
    
    def __str__(self):
        size = self.rows.shape[0]
        return "EXTEND ROWS BY {0}".format(size)
    
    def __repr__(self):
        return repr(self.rows)

# Add column to the end of dataframe
class ExtendColumns:
    __slots__ = ['id', 'columns']
    def __init__(self, columns: pandas.DataFrame):
        self.id = 'ext_cols'
        self.columns = columns
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = pandas.concat([data, self.columns], axis=1)
        return data
    
    def undo(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.drop(self.columns.name, axis=1)
        return data

    def __str__(self):
        size = self.columns.shape[1]
        return "EXTEND COLUMNS BY {0}".format(size)

    def __repr__(self):
        return repr(self.columns)

# Drop rows from dataframe
class DropRows:
    __slots__ = ['id', 'rows', 'ref']
    def __init__(self, rows: pandas.DataFrame, ref: pandas.Int64Index):
        self.id = 'drop_rows'
        self.rows = rows
        self.ref = ref
    
    def execute(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data = data.drop(self.rows.index)
        return data
    
    # *adds rows to original indices
    def undo(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data = data.append(self.rows)
        data = data.loc[ref]
        return data
    
    def __str__(self):
        size = self.rows.shape[0]
        return "DROP {0} ROW(S)".format(size)
    
    def __repr__(self):
        return repr(self.rows)

# Drop columns from dataframe
class DropColumns:
    __slots__ = ['id', 'columns', 'ref']
    def __init__(self, columns: pandas.DataFrame, ref: pandas.Index):
        self.id = 'drop_cols'
        self.columns = columns
        self.ref = ref
    
    def execute(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data = data.drop(self.columns.columns, axis=1)
        return data
    
    # *adds dropped columns to original positions
    def undo(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data = pandas.concat([data, self.columns], axis=1)
        data = data[self.ref]

        return data
    
    def __str__(self):
        size = self.columns.shape[1]
        return "DROP {0} COLUMN(S)".format(size)
    
    def __repr__(self):
        return repr(self.columns)

# Set dataframe index to another index of equal length
class Reindex:
    __slots__ = ['id', 'index_x', 'index_y']
    def __init__(self, index_x, index_y):
        self.id = 'reindex'
        self.index_x = index_x
        self.index_y = index_y
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data.index = self.index_y
        return data
    
    def undo(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data.index = self.index_x
        return data
    
    def __str__(self):
        return "REINDEX ROWS"
    
    def __repr__(self):
        return repr(self.index_y)

class RenameColumns:
    __slots__ = ['id', 'cols_x', 'cols_y']
    def __init__(self, cols_x, cols_y):
        self.id = 'rename'
        self.cols_x = cols_x
        self.cols_y = cols_y
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.rename(columns={
            cx: cy for cx, cy in zip(self.cols_x, self.cols_y)})
        return data
    
    def undo(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data = data.rename(columns={
            cy: cx for cx, cy in zip(self.cols_x, self.cols_y)})
        return data

    def __str__(self):
        return "RENAME COLUMNS"
    
    def __repr__(self):
        return repr(self.cols_y)