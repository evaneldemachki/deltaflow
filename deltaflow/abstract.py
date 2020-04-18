from typing import TypeVar, Iterable, Union
from abc import ABC, abstractmethod
import os

class Selection(ABC):
    def __init__(self, name: str, values: Iterable[object]):
        self.__name = name
        self.__values = tuple([v for v in values if v is not None])
    
    # return value of object at position i
    def _get(self, i: int) -> object:
        return self.__values[i]
    
    # return string repr. of an object at position i
    def _show(self, i: int) -> Union[str, None]:
        return repr(obj)
    
    def __getitem__(self, i: int) -> object:
        if i > len(self.__values) or i < 1:
            msg = "select from items {0}-{1}"
            raise IndexError(msg.format(1, len(self.__values)))
        
        return self._get(i - 1)

    def __str__(self):
        out = self.__name.upper() + ': {\n'
        
        i = 1
        for obj in self.__values:
            obj_str = self._show(i - 1)
            if obj_str is None:
                continue
            out += "  [{0}] {1}\n".format(i, obj_str)
            
            i += 1
        
        out += '}'
        return out
    
    __repr__ = __str__

class DirectoryMap(ABC):
    def __init__(self, path: str, name: str):
        self.__path = path
        self.__name = name
    
    @abstractmethod # parse file into object
    def _parse(self, text: str) -> object:
        pass

    # return string repr. of an object at key
    def _show(self, key: str, obj: object) -> str:
        return "{0}: {1}".format(key, repr(obj))
    
    def _read(self, fname: str) -> str:
        path = os.path.join(self.__path, fname)
        with open(path, 'r') as f:
            text = f.read()
        
        return text

    @property
    def dir(self):
        return os.listdir(self.__path)
    
    def items(self):
        items = []
        for key in self:
            items.append((key, self[key]))
        
        return items

    def __getitem__(self, key: str) -> object:
        try:
            text = self._read(key)
        except FileNotFoundError:
            raise KeyError(key)

        return self._parse(text)

    def __iter__(self):
        for key in self.dir:
            yield key

    def __contains__(self, key):
        return key in self.dir

    def __str__(self):
        out = self.__name.upper() + ': {\n'
        for key in self:
            out += "  {0}\n".format(self._show(key, self[key]))
        
        out += "}"
        return out
    
    __repr__ = __str__

