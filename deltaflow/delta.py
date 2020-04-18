from collections import OrderedDict
import deltaflow.operation as op
from deltaflow.block import AxisBlock, PutBlock, ExtensionBlock
import numpy

# Record drops & relabels in terms of base indices
def align(stage: 'Stage', diff: OrderedDict) -> OrderedDict:
    diff['drop'] = [None, None]
    diff['relabel'] = [None, None]

    base_index = [
        stage.base.index,
        stage.base.columns
    ]

    # record and align base section of dropped index at an axis
    def drop(index, axis):
        if diff['relabel'][axis] is not None:
            # get indexer for base relabel
            ix = diff['relabel'][axis].get_indexer(index)
            # remove extension indices
            ix = numpy.delete(ix, numpy.where(ix==-1))
            # drop entries from relabel record
            diff['relabel'] = diff['relabel'][axis].drop(index)
        else:
            # get indexer for base index
            ix = base_index[axis].get_indexer(index)
            # remove extension indices
            ix = numpy.delete(ix, numpy.where(ix==-1))

        # align drop indices to base index
        index = numpy.take(base_index[axis].to_numpy(), ix)
        # drop from base
        base_index[axis] = base_index[axis].drop(index)
        # add to or create diff record for drop index
        if diff['drop'][axis] is not None:
            diff['drop'][axis] = numpy.concatenate(
                diff['drop'][axis], index)
        else:
            diff['drop'][axis] = index

    # record base section of new index at an axis     
    def relabel(index, axis):
        base_slice = slice(0, base_index[axis].shape[0])
        diff['relabel'][axis] = index[base_slice]

    for oper in stage.iter_operations():
        if oper.id == 'drop':
            drop(op.effective_axis(oper.data, oper.axis), axis=oper.axis)
        elif oper.id == 'relabel':
            relabel(oper.y, axis=oper.axis)
    
    return diff

# Extract and record insertions & extensions
def extract(stage: 'Stage', diff: OrderedDict) -> OrderedDict:
    diff['put'] = None
    diff['extend'] = [None, None]

    x, y = stage.base, stage.live

    # apply any drops to x
    for axis in (0, 1):
        if diff['drop'][axis] is not None:
            x = x.drop(diff['drop'][axis], axis=axis)

    # extract any extensions from y and drop them
    for axis in (0, 1):
        if y.shape[axis] > x.shape[axis]:
            ext_slices = [slice(None), slice(None)]
            ext_slices[axis] = y._get_axis(axis)[x.shape[axis]:y.shape[axis]]
            diff['extend'][axis] = y.loc[ext_slices[0], ext_slices[1]]
            ext_slices[axis] = y._get_axis(axis)[0:x.shape[axis]]
            y = y.loc[ext_slices[0], ext_slices[1]]

    # realign y labels to x labels for relabeled axes
    for axis in (0, 1):
        if diff['relabel'][axis] is not None:
            y = y.set_axis(x._get_axis(axis), axis=axis)
    
    # check for data type preservation
    put_values = op.shrink(x, y)
    y_of_put = y.loc[put_values.index, put_values.columns]
    dt_pres = (y_of_put.dtypes != put_values.dtypes)
    dt_pres = y_of_put[dt_pres[dt_pres].index].dtypes
    dt_pres = dt_pres
    if len(dt_pres) == 0:
        dt_pres = None
    if put_values.shape[0] != 0:
        diff['put'] = (put_values, dt_pres)
    
    return diff

# Convert diff entries into their associated blocks
def build(stage: 'Stage') -> OrderedDict:
    diff = {}
    diff = align(stage, diff)
    diff = extract(stage, diff)

    delta = OrderedDict()

    check = (diff['drop'], diff['relabel'])
    cond = [any(c[i] is not None for i in (0, 1)) for c in check]
    if any(cond):
        has_drop, has_relabel = cond
        drop_sec = diff['drop'] if has_drop else None
        relabel_sec = diff['relabel'] if has_relabel else None
        delta['axis'] = AxisBlock(drop_sec, relabel_sec)
    if diff['put'] is not None:
        delta['put'] = PutBlock(diff['put'][0], dtypes=diff['put'][1])
    if diff['extend'][0] is not None or diff['extend'][1] is not None:
        delta['extend'] = ExtensionBlock(diff['extend'][1], diff['extend'][0])

    return delta


    

    

    

    

    





    



            