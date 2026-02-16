from .dataset import Dataset
from .local_dataset import LocalDataset
from .remote_dataset import RemoteDataset
from .memory_dataset import MemoryDataset
from ..spatial.template_manager import TemplateManager
try:
    from .remote_dataset import S3Dataset, SFTPDataset
except ImportError:
    pass