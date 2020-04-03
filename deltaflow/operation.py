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
    diff = diff[~diff.isna().all(axis=1)]

    return diff

class Operation:
    pass

class Update(Operation):
    def __init__(self, data_x, data_y):
        self.data_x = data_x
        self.data_y = data_y
    
    def execute(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data.update(self.data_y)
        return data
    
    def undo(self, data:pandas.DataFrame) -> pandas.DataFrame:
        data.update(self.data_x)
        return data

# Drop rows from dataframe
class DropRows(Operation):
    def __init__(self, rows: pandas.DataFrame, ref: pandas.Int64Index):
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

# Add rows to the end of dataframe
class AddRows(Operation):
    def __init__(self, rows: pandas.DataFrame):
        self.rows = rows
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.append(self.rows)
        return data
    
    def undo(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.drop(self.rows.index)
        return data

# Add column to the end of dataframe
class AddColumns(Operation):
    def __init__(self, columns: pandas.DataFrame):
        self.columns = columns
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = pandas.concat([data, self.columns], axis=1)
        return data
    
    def undo(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data = data.drop(self.columns.name, axis=1)
        return data

# Drop columns from dataframe
class DropColumns(Operation):
    def __init__(self, columns: pandas.DataFrame, ref: pandas.Index):
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

# Set dataframe index to another index of equal length
class Reindex(Operation):
    def __init__(self, index_x, index_y):
        self.index_x = index_x
        self.index_y = index_y
    
    def execute(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data.index = self.index_y
        return data
    
    def undo(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data.index = self.index_x
        return data

class RenameColumns(Operation):
    def __init__(self, cols_x: pandas.Index, cols_y:pandas.Index):
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