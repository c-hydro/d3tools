from .dataset import Dataset
from .local_dataset import LocalDataset
from .template_manager import TemplateManager
try:
    from .remote_dataset import S3Dataset, SFTPDataset
except ImportError:
    pass