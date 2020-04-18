import pandas
import numpy
from typing import Iterable, Union

DataFrame = pandas.DataFrame
Series = pandas.Series
Index = pandas.Index

# return effective axis for pandas object
def effective_axis(obj: Union[DataFrame, Series], axis=int) -> Index:
    if axis == 0: # row index
        ix = obj.index
    else: # column index
        if isinstance(obj, Series):
            ix = pandas.Index([obj.name])
        elif isinstance(obj, DataFrame):
            ix = obj.columns
        else:
            raise TypeError

    return ix

# return reduced dataframe of difference from x to y
def shrink(x: DataFrame, y: DataFrame) -> DataFrame:
    diff = y[~y.isin(x)]
    diff = diff.dropna(axis='columns', how='all')
    diff = diff.dropna(axis='rows', how='all')

    return diff

# Put non-NA values from y into DataFrame
class Put:
    __slots__ = ['id', 'x', 'y', 'dtypes']
    def __init__(self, x: DataFrame, y: DataFrame, dtypes: Series):
        self.id = 'put'
        self.x = x
        self.y = y
        self.dtypes = dtypes

    def execute(self, data: DataFrame) -> DataFrame:
        data = data.copy()
        data.update(self.y)
        data = data.astype(self.dtypes[self.y.columns])
        return data
    
    # replace segment y with original segment x
    def undo(self, data:pandas.DataFrame) -> DataFrame:
        data.update(self.x)
        data = data.astype(self.dtypes[self.x.columns])

        return data
    
    def __str__(self):
        total = self.y.count().sum()
        return "PUT {0} VALUES".format(total)

    def __repr__(self):
        return repr(self.y)

# Add rows/columns to the end of DataFrame
class Extend:
    __slots__ = ['id', 'data', 'axis']
    def __init__(self, data: DataFrame, axis: int):
        self.id = 'extend'
        self.data = data
        self.axis = axis
    
    def execute(self, data: DataFrame) -> DataFrame:
        data = pandas.concat([data, self.data], axis=self.axis)
        return data
    
    # drop extension indices from DataFrame
    def undo(self, data: DataFrame) -> DataFrame:
        data = data.drop(
            self.data._get_axis(self.axis), 
            axis=self.axis
        )
        return data

    def __str__(self):
        size = self.data.shape[self.axis]
        if self.axis == 0:
            return "EXTEND ROWS BY {0}".format(size)
        else:
            return "EXTEND COLUMNS BY {0}".format(size)

    def __repr__(self):
        return repr(self.data.index)

# Drop rows/columns from DataFrame
class Drop:
    __slots__ = ['id', 'data', 'ref', 'axis']
    def __init__(self, data: DataFrame, ref: Iterable, axis: int):
        self.id = 'drop'
        self.data = data
        self.ref = ref
        self.axis = axis
    
    def execute(self, data: DataFrame) -> DataFrame:
        data = data.drop(self.data._get_axis(self.axis), self.axis)
        return data
    
    # add dropped indices back to their original positions
    def undo(self, data: DataFrame) -> DataFrame:
        data = pandas.concat([data, self.data], axis=self.axis)
        data = data.reindex(self.ref, axis=self.axis)
        return data
    
    def __str__(self):
        size = self.data.shape[self.axis]
        if self.axis == 0:
            return "DROP {0} ROW(S)".format(size)
        else:
            return "DROP {0} COLUMN(S)".format(size)
    
    def __repr__(self):
        return repr(self.data)

# Replace DataFrame axis labels with y
class Relabel:
    __slots__ = ['id', 'x', 'y', 'axis']
    def __init__(self, x: Iterable, y: Iterable, axis: int):
        self.id = 'relabel'
        self.x = x
        self.y = y
        self.axis = axis

    def execute(self, data: DataFrame) -> DataFrame:
        data = data.set_axis(self.y, axis=self.axis)
        return data
    
    # set DataFrame axis labels back to x
    def undo(self, data: DataFrame) -> DataFrame:
        data = data.set_axis(self.x, axis=self.axis)
        return data
    
    def __str__(self):
        if self.axis == 0:
            return "RELABEL ROWS"
        else:
            return "RELABEL COLUMNS"
    
    def __repr__(self):
        return repr(self.y)