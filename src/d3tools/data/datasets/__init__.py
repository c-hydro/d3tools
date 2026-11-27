"""Storage-specific dataset implementations used by d3tools.

The package exports the base dataset abstraction together with concrete local,
remote, and in-memory backends used by workflow code to read and write data.
"""

from .dataset import Dataset
from .local_dataset import LocalDataset
from .remote_dataset import RemoteDataset
from .memory_dataset import MemoryDataset

__all__ = ['Dataset', 'LocalDataset', 'RemoteDataset', 'MemoryDataset']