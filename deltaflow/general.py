import pandas

# return column index for pandas object
def column_index(obj):
    if isinstance(obj, pandas.Series):
        cols = pandas.Index([obj.name])
    elif isinstance(obj, pandas.DataFrame):
        cols = pandas.Index(obj.columns)
    else: raise PHError

    return cols

# return reduced dataframe of modified rows from x to y
def shrink(x, y):
    diff = y[~y.isin(x)]
    diff = diff[~diff.isna().all(axis=1)]

    return diff