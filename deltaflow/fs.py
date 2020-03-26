import os
import json
import struct
import pandas
from io import BufferedReader
from typing import Tuple, Any, TypeVar, IO, BinaryIO
from fastparquet import ParquetFile, write
from fastparquet.writer import make_metadata, check_column_names, write_thrift, make_row_group
from deltaflow.errors import *
from deltaflow.delta import block_order

class PHError(Exception):
    pass

OrderedDict = TypeVar('OrderedDict')
Block = TypeVar('DeltaBlock')

class DeltaIO:
    def __init__(self, obj, chunks):
        self.obj = obj
        self.chunks = chunks
        self._cursor = 0

        self.obj.seek(0)

    @property
    def cursor(self):
        return self._cursor
    
    @cursor.setter
    def cursor(self, val):
        self._cursor = val
        self.obj.seek(self.start())

    def tell(self):
        start = self.start()
        mark = self.obj.tell() - start

        return mark
    
    def write(self, content):
        self.obj.write(content)
    
    def add_chunk(self, chunk):
        self.chunks.append(chunk)

    def start(self):
        mark = sum(self.chunks[:self.cursor])
        return mark
    
    def end(self):
        mark = sum(self.chunks[:self.cursor + 1])
        return mark
    
    def read(self, n=None):
        if n is None:
            end = self.end()
            mark = self.obj.tell()
            n2 = end - mark
        else:
            limit = self.end() - self.start() - self.tell()
            if n > limit:
                n2 = limit
            else:
                n2 = n

        res = self.obj.read(n2) 

        return res
    
    def seek(self, n, mode=None):
        if mode is None:
            out = n
            mark = self.start() + n
            self.obj.seek(mark)
        elif mode == 2:
            out = self.end() + n - self.start()
            mark = self.end() + n
            self.obj.seek(mark)

        return out

    def __enter__(self, *ignore):
        return self

    def __exit__(self, *ignore):
        pass

def write_block(data: pandas.DataFrame, fobj: BinaryIO) -> int:
    count = fobj.tell()

    row_group_offsets=50000000
    l = len(data)
    nparts = max((l - 1) // row_group_offsets + 1, 1)
    chunksize = max(min((l - 1) // nparts + 1, l), 1)
    row_group_offsets = list(range(0, l, chunksize))

    if not isinstance(data.index, pandas.RangeIndex):
        cols = set(data)
        data = data.reset_index()
        index_cols = [c for c in data if c not in cols]
    elif isinstance(data.index, pandas.RangeIndex):
        # range to metadata
        index_cols = data.index
    else:
        index_cols = []
    
    check_column_names( # partition_on, fixed_text, object_encoding, has_nulls
        data.columns, [], None, 'infer', True)

    fmd = make_metadata(
        data, fixed_text=None, object_encoding='infer',
        times='int64', index_cols=index_cols
    )
    
    fobj.write(b'PAR1')
    for i, start in enumerate(row_group_offsets):
        end = row_group_offsets[i+1] if i < (len(row_group_offsets) - 1) else None
        rg = make_row_group(fobj, data[start:end], fmd.schema, compression=None)
        if rg is not None:
            fmd.row_groups.append(rg)

    foot_size = write_thrift(fobj, fmd)
    fobj.write(struct.pack(b"<i", foot_size))
    fobj.write(b'PAR1')
    count = fobj.tell() - count
    return count

def write_delta(path: str, node_id: str, make: OrderedDict):
    fpath = os.path.join(path, 'deltas', node_id + '.delta')
    # constant byte size, boolean array which is written to the end of delta file
    tail = struct.pack( # used during file read to parse chunk index
        '?' * len(make), # one true/false value for each make entry
        *[True if make[key] is not False
            else False for key in make]
    )
    blocks = [key for key in make if make[key] is not False]
    chunksfs = 'q' * len(blocks)
    chunks = []
    with open(fpath, 'wb') as deltafile:
        delta_io = DeltaIO(deltafile, chunks)
        for key in blocks:
            # write block to file
            delta_io.add_chunk(write_block(make[key], delta_io))
            delta_io.cursor += 1

        chunks = struct.pack(chunksfs, *chunks)
        delta_io.write(chunks)
        delta_io.write(tail)

# Generator to return delta block keys and data 
def iter_delta(path: str, node_id: str) -> Tuple[str, Block]:
    delta_path = os.path.join(path, 'deltas', node_id + '.delta')
    delta_size = os.path.getsize(delta_path)
    with open(delta_path, 'rb') as deltafile:
        tailfs = '?' * len(block_order)
        n = struct.calcsize(tailfs)
        # seek to last n bytes to read tail
        deltafile.seek(-n, os.SEEK_END)
        tail = struct.unpack(tailfs, deltafile.read())
        # seek to the last n + m bytes to read chunk index
        # map tail values to block_order, create meta indexer
        indexer = [block_order[i] for i in range(len(block_order)) if tail[i] is not False]
        chunksfs = 'q' * len(indexer)
        m = struct.calcsize(chunksfs)
        deltafile.seek(-(n + m), os.SEEK_END)
        chunks = struct.unpack(chunksfs, deltafile.read(m))
        # generator
        deltafile.seek(0)
        end = delta_size - n - m
        delta_io = DeltaIO(deltafile, chunks)
        for key in indexer:
            block = ParquetFile('null', open_with=lambda *ignore: delta_io)
            delta_io.seek(0)
            bp = block.to_pandas()
            delta_io.cursor += 1
            yield key, bp

def write_origin(path: str, name: str, data: pandas.DataFrame):
    origin_path = os.path.join(path, name + '.origin')
    if os.path.isfile(origin_path):
        raise OriginFileNameError(name)
    
    write(origin_path, data)

def load_origin(path: str, name: str) -> pandas.DataFrame:
    origin_path = os.path.join(
        os.path.dirname(path), name + '.origin')
    data = ParquetFile(origin_path).to_pandas()
    return data
