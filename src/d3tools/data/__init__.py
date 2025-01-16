from .dataset import Dataset
from .local_dataset import LocalDataset
try:
    from .remote_dataset import S3Dataset, SFTPDataset
except ImportError:
    pass