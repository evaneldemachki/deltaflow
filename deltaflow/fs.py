import os
import json
import struct
import pandas
from typing import Tuple, List, TypeVar, IO, BinaryIO
import fastparquet
from deltaflow.errors import NameExistsError
from deltaflow.delta import block_order
from collections import OrderedDict

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

def delta_reader(path: str, node_id: str) -> BinaryIO:
    delta_path = os.path.join(path, 'deltas', node_id + '.delta')
    deltafile = open(delta_path, 'rb')
    
    return deltafile

# Iterates through delta make, write delta file
def write_delta(path: str, node_id: str, make: OrderedDict):
    fpath = os.path.join(path, 'deltas', node_id + '.delta')
    meta = OrderedDict()
    with open(fpath, 'wb') as deltafile:
        delta_io = DeltaIO(deltafile, [])
        for key in make:
            # write each block payload as to parquet
            payload = make[key].payload
            fastparquet.write('null', payload, open_with=lambda *ignore: delta_io)
            chunk_size = delta_io.tell()
            delta_io.add_chunk(chunk_size)
            # log chunk size to block meta
            make[key].meta['chunk'] = chunk_size
            # write block meta to meta list
            meta[key] = make[key].meta
            delta_io.cursor += 1

        # convert meta to utf-8 encoded JSON string
        meta = json.dumps(meta).encode('utf-8')
        # calculate encoded meta size pack into 8-byte long long struct
        tail = struct.pack('q', len(meta))
        # write meta followed by tail
        delta_io.write(meta)
        delta_io.write(tail)

# Yields key, block pairs on each iteration given delta file
def iter_delta(path: str, node_id: str) -> Tuple[str, Block]:
    deltafile = delta_reader(path, node_id)
    with deltafile as deltafile:
        # read chunk meta
        meta = read_meta(deltafile)
        chunks = [meta[key]['chunk'] for key in meta]
        # block generator
        deltafile.seek(0)
        delta_io = DeltaIO(deltafile, chunks)
        for key in meta:
            block = fastparquet.ParquetFile(
                'null', open_with=lambda *ignore: delta_io)
            delta_io.seek(0)
            block = block.to_pandas()
            delta_io.cursor += 1
            yield meta[key], block

# Return individual block of delta file given key
def read_block(path: str, node_id: str, i: int) -> Tuple[str, Block]:
    deltafile = delta_reader(path, node_id)
    with deltafile as deltafile:
        meta = read_meta(deltafile)
        key = list(meta)[i]
        chunks = [meta[key]['chunk'] for key in meta]
        # read block
        deltafile.seek(0)
        delta_io = DeltaIO(deltafile, chunks)
        delta_io.cursor = i
        block = fastparquet.ParquetFile(
            'null', open_with=lambda *ignore: delta_io)

        block = block.to_pandas()
    
    return meta[key], block

# Read chunk meta data of a delta file
def read_meta(deltafile: BinaryIO) -> OrderedDict:
    # seek to last 8 bytes to read tail
    deltafile.seek(-8, os.SEEK_END)
    tail = struct.unpack('q', deltafile.read())[0]
    # decode meta from (-tail - 8, -8) bytes of deltafile
    deltafile.seek(-tail - 8, os.SEEK_END)
    meta = deltafile.read(tail).decode('utf-8')
    meta = json.loads(meta, object_pairs_hook=OrderedDict)

    return meta

def write_origin(path: str, name: str, data: pandas.DataFrame):
    origin_path = os.path.join(path, name + '.origin')
    if os.path.isfile(origin_path):
        raise NameExistsError('origin', name)
    
    fastparquet.write(origin_path, data)

def load_origin(path: str, name: str) -> pandas.DataFrame:
    origin_path = os.path.join(
        os.path.dirname(path), name + '.origin')
    data = fastparquet.ParquetFile(origin_path).to_pandas()
    if data.index.name == 'index':
        data.index.name = None

    return data