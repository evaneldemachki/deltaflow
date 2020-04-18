![DeltaFlow Logo](https://repository-images.githubusercontent.com/250386104/52d02a00-75f6-11ea-82e9-9f6ea27ea6a2)
# DeltaFlow
## Decentralized version control for pandas
DeltaFlow is an attempt to rethink the way changes in data are represented, both in-memory and on-disk. Rather than storing the entire state of a data set each time a change is applied, it calculates and compresses the difference between any two states and adds it to a hierarchial tree of references.
### Basics
Using it is as simple as initializing a DeltaFlow field in a given directory, saving a pandas DataFrame as an origin, and loading it from disk as an arrow (example usage below). From there on, changes can be applied and commited--each commit writing its difference from its parent commit to disk, and a node to a tree of references. The state of the DataFrame at any commit can always be quickly re-visited, and the state of the origin DataFrame is never changed.
### The Arrow
The arrow is the fundamental abstraction which facilitates all changes to a data set via a handful of class methods which can be called to apply modifications in a manner that allows DeltaFlow to keep track of the their underlying nature.
### Integrity
Inspired by Git, the state of the data at any commit is represented by a SHA-1 hash of its content paired with the string representation of its associated node, assuring that any unintentional alterations in saved data are detected on load.
## Installation
<i>Note: this project is currently in its infancy and likely very unstable.</i>

DeltaFlow can currently be installed via [pip](https://pip.pypa.io/en/stable/) using this repo.
```bash
pip install git+https://github.com/evaneldemachki/deltaflow.git
```
This project is dependent on [fastparquet](https://fastparquet.readthedocs.io/en/latest/) which requires Visual C++ 2014 in order to build. Otherwise, pre-compiled fastparquet wheels can be found [here](https://www.lfd.uci.edu/~gohlke/pythonlibs/)
## Documentation
Basic example [here](https://github.com/evaneldemachki/deltaflow/blob/master/docs/example.ipynb/)
