import xarray as xr
import pandas as pd
import tempfile
from typing import Optional
import boto3
import os

try:
    from .dataset import Dataset
    from .io_utils import write_to_file, read_from_file
    from ..config.parse import extract_date_and_tags
except ImportError:
    from dataset import Dataset
    from io_utils import write_to_file, read_from_file
    from config.parse import extract_date_and_tags

class S3Dataset(Dataset):
    type = 's3'

    def __init__(self,
                 key_pattern: str,
                 bucket_name: str,
                 tmp_dir: Optional[str] = tempfile.gettempdir(),
                 keep_after_reading = False, **kwargs):
        self.key_pattern = key_pattern
        super().__init__(**kwargs)
        self.local_keymap = {}
        self.bucket_name = bucket_name
        self.tmp_dir = tempfile.mkdtemp(dir = tmp_dir)

        self.s3_client = boto3.client('s3')

    @property
    def key_pattern(self):
        return self._key_pattern

    @key_pattern.setter
    def key_pattern(self, key_pattern):
        self._key_pattern = key_pattern

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_key):
        local_key = os.path.join(self.tmp_dir, input_key)
        if local_key not in self.local_keymap:
            self.s3_client.download_file(self.bucket_name, input_key, local_key)
            self.local_keymap[input_key] = local_key

        return read_from_file(local_key, self.format)

    def _write_data(self, output: xr.DataArray|pd.DataFrame, output_key: str):
        local_key = os.path.join(self.tmp_dir, output_key)
        write_to_file(output, local_key, self.format)
        self.s3_client.upload_file(local_key, self.bucket_name, output_key)
        self.local_keymap[output_key] = local_key

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        self.s3_client.head_object(Bucket = self.bucket_name, Key = data_path)

    @property
    def available_keys(self):
        prefix = self.key_pattern
        while '%' in prefix or '{' in prefix:
            prefix = os.path.dirname(prefix)
        
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket = self.bucket_name, Prefix = prefix):
            content = page.get('Contents', [])
            for file in content:
                try:
                    extract_date_and_tags(file['Key'], self.key_pattern)
                    files.append(file['Key'])
                except ValueError:
                    pass
        
        return files

    def update(self, in_place = False, **kwargs):
        new_self = super().update(in_place = in_place, **kwargs)

        for key in self.local_keymap:
            try: 
                extract_date_and_tags(key, new_self.key_pattern)
                new_self.local_keymap[key] = self.local_keymap.get(key)
            except ValueError:
                pass

        if in_place:
            self = new_self
            return self
        else:
            return new_self
