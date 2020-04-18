import os
import json
import struct
import pandas
import numpy
import fastparquet
from typing import Tuple, List, TypeVar, BinaryIO, Callable
from collections import OrderedDict
from deltaflow.errors import NameExistsError
from deltaflow.block import get_block

BlockObject = TypeVar('DeltaBlock')
Modifier = Callable[[pandas.DataFrame], pandas.DataFrame]

# Chunk writer for delta files
class DeltaIO: # masks each block as a seperate file
    def __init__(self, obj: BinaryIO, chunks: List[Tuple[int]]):
        self.obj = obj
        self.chunks = chunks
        self._cursor = 0
        self._part = 0

        self.obj.seek(0)

    @property # index of current chunk
    def cursor(self) -> int:
        return self._cursor
    
    @cursor.setter # set current chunk
    def cursor(self, val) -> None:
        self._cursor = val
        self._part = 0
        self.obj.seek(self.bounds[0])
    
    def seekable(self) -> bool:
        return True
    
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

    # get file location relative to chunk bounds
    def tell(self) -> int:
        mark = self.obj.tell() - self.bounds[0]
        return mark
    
    def flush(self) -> None:
        self.obj.flush()

    # return self inside of external with statement
    def __enter__(self, *ignore): # (instead of actual file)
        return self

    # prevents closing inside of external with statement
    def __exit__(self, *ignore):
        pass

class DeltaReader(DeltaIO):
    def __init__(self, obj: BinaryIO, chunks: List[Tuple[int]]):
        super().__init__(obj, chunks)

    # TODO: change this to cache    
    @property
    def bounds(self) -> Tuple[int]:
        chunk_start = sum(sum(i) for i in self.chunks[:self._cursor])
        block = self.chunks[self._cursor]

        lower = chunk_start + sum(block[:self._part])
        upper = lower + sum(block[:self._part + 1])

        return lower, upper

    # shift to next partition in current chunk
    def next(self) -> None:
        self._part += 1
        self.obj.seek(self.bounds[0])

class DeltaWriter(DeltaIO):
    def __init__(self, obj: BinaryIO):
        self.queue = []
        super().__init__(obj, chunks=[])

    # override for standard write method
    def write(self, content: bytes) -> int:
        res = self.obj.write(content)
        return res

    # TODO: change this to cached
    @property # chunk bounds
    def bounds(self) -> Tuple[int, int]:
        chunk_start = sum(sum(i) for i in self.chunks[:self._cursor])

        if self._part == len(self.queue):
            lower = chunk_start
            upper = None
        else:
            lower = chunk_start + sum(self.queue[:-1])
            upper = lower + self.queue[-1]
        
        return lower, upper
    
    # if currently writing partition, override base class method
    def seek(self, n: int, mode: int = 0) -> int:
        if self._part == len(self.queue): # no upper limits
            bounds = self.bounds
            if mode == 0:
                mark = bounds[0] + n
                res = n
                self.obj.seek(mark)
            elif mode == 1:
                res = self.tell() + n
                self.obj.seek(n, 1)
            elif mode == 2:
                self.obj.seek(n, os.SEEK_END)
                res = self.tell()
                if res < 0:
                    raise OSError('[Errno 22] Invalid argument')
        else: # revert to DeltaIO.seek
            res = super().seek(n, mode)

        return res

    # shift to next partition in current chunk
    def next(self) -> None:
        self.obj.seek(0, os.SEEK_END)
        offset = self.obj.tell() - self.bounds[0]
        self.queue.append(offset)
    
    # add queue to chunks, reset queue, return chunk
    def push(self) -> Tuple[int]:
        chunk = tuple(self.queue)
        self.chunks.append(chunk)
        self.queue = []

        return chunk

class DeltaFile:
    def __init__(self, path: str, node_id: str):
        self.path = os.path.join(path, 'deltas', node_id + '.delta')
        self.meta = self.read_meta()

    # Return individual block of delta file given key
    def read_block(self, i: int) -> BlockObject:
        meta = self.meta
        chunks = [tuple(meta[key]['chunk']) for key in meta]
        key = list(meta)[i]
        block = get_block(meta[key]['class'])
        with open(self.path, 'rb') as delta_file:
            reader = DeltaReader(delta_file, chunks)
            reader.cursor = i
            obj = block.parse(meta[key], reader)
        
        return obj

    # Yields key, block pairs on each iteration given delta file
    def iter_blocks(self) -> Modifier:
        meta = self.meta
        chunks = [meta[key]['chunk'] for key in meta]
        with open(self.path, 'rb') as delta_file:
            reader = DeltaReader(delta_file, chunks)
            # block generator
            i = 0
            for key in meta:
                reader.cursor = i
                block = get_block(meta[key]['class'])
                obj = block.parse(meta[key], reader)
                
                modifier = lambda df: block.apply(meta[key], obj, df)
                yield modifier

                i += 1

    # Read chunk meta data of a delta file
    def read_meta(self) -> OrderedDict:
        with open(self.path, 'rb') as delta_file:
            # seek to last 8 bytes to read tail (size of meta in bytes)
            delta_file.seek(-8, os.SEEK_END) 
            tail = struct.unpack('q', delta_file.read())[0]
            # decode meta from (-tail - 8, -8) bytes of deltafile
            delta_file.seek(-tail - 8, os.SEEK_END)
            meta = delta_file.read(tail).decode('utf-8')
            meta = json.loads(meta, object_pairs_hook=OrderedDict)

        return meta

# Iterates through delta blocks, write delta file
def write_delta(path: str, node_id: str, delta: OrderedDict):
    fpath = os.path.join(path, 'deltas', node_id + '.delta')
    meta = OrderedDict()
    with open(fpath, 'wb') as delta_file:
        writer = DeltaWriter(delta_file)
        for key in delta:
            # call write method of each block -> writes partitions to queue
            block = delta[key]
            block.write(writer)
            # write block meta to meta list
            meta[key] = delta[key].meta

            writer.cursor += 1

        # convert meta to utf-8 encoded JSON string
        meta = json.dumps(meta).encode('utf-8')
        # write encoded meta size into 8-byte long long struct
        tail = struct.pack('q', len(meta))
        # write meta followed by tail
        writer.write(meta)
        writer.write(tail)

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
    data = data.fillna(value=numpy.nan)

    return data