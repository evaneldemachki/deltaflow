import os
import json
import struct
import pandas
from typing import Tuple, List, TypeVar, IO, BinaryIO
import fastparquet
from deltaflow.errors import NameExistsError
from deltaflow.delta import block_order

OrderedDict = TypeVar('OrderedDict')
Block = TypeVar('DeltaBlock')

# Chunk reader & writer for delta files
class DeltaIO: # masks each block as a seperate file
    def __init__(self, obj: BinaryIO, chunks: List[int]):
        self.obj = obj
        self.chunks = chunks
        self._cursor = 0

        self.obj.seek(0)

    @property # chunk bounds
    def bounds(self) -> Tuple[int, int]:
        lower = sum(self.chunks[:self.cursor])
        upper = sum(self.chunks[:self.cursor + 1])
        return (lower, upper)

    @property # index of current chunk
    def cursor(self) -> int:
        return self._cursor
    
    @cursor.setter # set current chunk
    def cursor(self, val) -> None:
        self._cursor = val
        self.obj.seek(self.bounds[0])

    # get file location relative to chunk bounds
    def tell(self) -> int:
        mark = self.obj.tell() - self.bounds[0]
        return mark
    
    # override for standard write method
    def write(self, content: bytes) -> int:
        res = self.obj.write(content)
        return res
    
    # add chunk to index
    def add_chunk(self, chunk: int) -> None:
        self.chunks.append(chunk)

    # read within (and relative to) current chunk
    def read(self, n: int = None) -> bytes:
        bounds = self.bounds
        if n is None:
            section = bounds[1] - self.obj.tell()
        else:
            limit = bounds[1] - self.obj.tell()
            section = n if n < limit else limit

        res = self.obj.read(section) 
        return res
    
    # seek within (and relative to) current chunk
    def seek(self, n: int, mode: int = 0) -> int:
        bounds = self.bounds
        if mode == 0: # seek relative to lower bound
            limit = bounds[1] - bounds[0]
            mark = bounds[0] + n if n < limit else bounds[1]
            res = n if n < limit else limit
            self.obj.seek(mark)
        elif mode == 1: # seek relative to current position
            limit = bounds[1] - self.obj.tell()
            mark = n if n < limit else limit
            res = self.tell() + mark
            self.obj.seek(mark, 1)
        elif mode == 2: # seek relative to upper bound
            limit = bounds[0] - bounds[1]
            mark = bounds[1] + n if n > limit else bounds[0]
            res = n if n > limit else limit
            self.obj.seek(mark)

        return res

    # return self inside of external with statement
    def __enter__(self, *ignore): # (instead of actual file)
        return self

    # prevents closing inside of external with statement
    def __exit__(self, *ignore):
        pass

# Iterates through delta make, write delta file
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
            # write each block as parquet file using fastparquet
            fastparquet.write('null', make[key], open_with=lambda *ignore: delta_io)
            delta_io.add_chunk(delta_io.tell())
            delta_io.cursor += 1
        # write chunks and tail to end of delta file
        chunks = struct.pack(chunksfs, *chunks)
        delta_io.write(chunks)
        delta_io.write(tail)

# Yields key, block pairs on each iteration given delta file
def iter_delta(path: str, node_id: str) -> Tuple[str, Block]:
    delta_path = os.path.join(path, 'deltas', node_id + '.delta')
    delta_size = os.path.getsize(delta_path)
    with open(delta_path, 'rb') as deltafile:
        tailfs = '?' * len(block_order)
        n = struct.calcsize(tailfs)
        # seek to last n bytes to read tail
        deltafile.seek(-n, os.SEEK_END)
        tail = struct.unpack(tailfs, deltafile.read())
        # map tail values to block_order, create meta indexer
        indexer = [block_order[i] for i in range(len(block_order)) if tail[i] is not False]
        chunksfs = 'q' * len(indexer)
        m = struct.calcsize(chunksfs)
        # seek to the last n + m bytes, read m-byte chunk index
        deltafile.seek(-(n + m), os.SEEK_END)
        chunks = struct.unpack(chunksfs, deltafile.read(m))
        # block generator
        deltafile.seek(0)
        end = delta_size - n - m
        delta_io = DeltaIO(deltafile, chunks)
        for key in indexer:
            block = fastparquet.ParquetFile(
                'null', open_with=lambda *ignore: delta_io)
            delta_io.seek(0)
            bp = block.to_pandas()
            delta_io.cursor += 1
            yield key, bp

def write_origin(path: str, name: str, data: pandas.DataFrame):
    origin_path = os.path.join(path, name + '.origin')
    if os.path.isfile(origin_path):
        raise NameExistsError('origin', name)
    
    fastparquet.write(origin_path, data)

def load_origin(path: str, name: str) -> pandas.DataFrame:
    origin_path = os.path.join(
        os.path.dirname(path), name + '.origin')
    data = fastparquet.ParquetFile(origin_path).to_pandas()
    return data
