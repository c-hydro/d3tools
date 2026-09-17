from .datasets import (
    Dataset,
    LocalDataset,
    RemoteDataset,
    MemoryDataset
)

from ..spatial.template_manager import TemplateManager

try:
    from .datasets.remote_dataset import S3Dataset, SFTPDataset
except ImportError:
    pass

from .datasets import dataset as _dataset_mod
from .datasets import local_dataset as _local_dataset_mod
from .datasets import memory_dataset as _memory_dataset_mod
from .datasets import remote_dataset as _remote_dataset_mod

import sys

# Legacy module paths -> new modules
sys.modules.setdefault(__name__ + ".dataset", _dataset_mod)
sys.modules.setdefault(__name__ + ".local_dataset", _local_dataset_mod)
sys.modules.setdefault(__name__ + ".memory_dataset", _memory_dataset_mod)
sys.modules.setdefault(__name__ + ".remote_dataset", _remote_dataset_mod)

# Also expose them as attributes of d3tools.data
dataset = _dataset_mod
local_dataset = _local_dataset_mod
memory_dataset = _memory_dataset_mod
remote_dataset = _remote_dataset_mod