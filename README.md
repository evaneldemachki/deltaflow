![DeltaFlow Logo](https://repository-images.githubusercontent.com/250386104/6b619400-6fa7-11ea-9443-e2675c49a252)
# DeltaFlow
DeltaFlow is a Python library that brings decentralized version control to the Pandas DataFrame. 

Inspired by GIT, series of changes are reduced to only their new and modified components. These changes are compressed and added to a file-based tree of references to previous states. Likewise, each state is translated into a unique hash identifier and used at runtime to verify the integrity of the resultant data.
## Installation
<i>Note: this project is currently in its infancy and likely very unstable.</i>

DeltaFlow can currently be installed via [pip](https://pip.pypa.io/en/stable/) using this repo.
```bash
pip install git+https://github.com/evaneldemachki/deltaflow.git
```
This project is dependent on [fastparquet](https://fastparquet.readthedocs.io/en/latest/) which requires Visual C++ 2014 in order to build. Otherwise, pre-compiled fastparquet wheels can be found [here](https://www.lfd.uci.edu/~gohlke/pythonlibs/)
## Usage
Basic example [here](https://github.com/evaneldemachki/deltaflow/blob/master/example.ipynb/)
