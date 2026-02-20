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