import xarray as xr
import pandas as pd
import tempfile
from typing import Optional
import boto3
import os
from functools import cached_property
import shutil
import atexit

try:
    from .dataset import Dataset
    from .io_utils import write_to_file, read_from_file
    from ..config.parse_utils import extract_date_and_tags
except ImportError:
    from dataset import Dataset
    from io_utils import write_to_file, read_from_file
    from config.parse_utils import extract_date_and_tags

class S3Dataset(Dataset):
    type = 's3'

    def __init__(self,
                 key_pattern: str,
                 bucket_name: str,
                 tmp_dir: Optional[str] = None,
                 **kwargs):
        self.key_pattern = key_pattern
        self.bucket_name = bucket_name
        if tmp_dir:
            os.makedirs(tmp_dir, exist_ok = True)
            self.tmp_dir = tmp_dir
        else:
            self.tmp_dir = tempfile.mkdtemp()

        self.s3_client = boto3.client('s3')
        self.available_keys_are_cached = False

        super().__init__(**kwargs)

        atexit.register(self.cleanup)

    @property
    def key_pattern(self):
        return self._key_pattern

    @key_pattern.setter
    def key_pattern(self, key_pattern):
        self._key_pattern = key_pattern

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_key):
        local_key = os.path.join(self.tmp_dir, input_key)
        if not os.path.exists(local_key):
            os.makedirs(os.path.dirname(local_key), exist_ok = True)
            self.s3_client.download_file(self.bucket_name, input_key, local_key)

        return read_from_file(local_key, self.format)

    def _write_data(self, output: xr.DataArray|pd.DataFrame, output_key: str, **kwargs):
        local_key = os.path.join(self.tmp_dir, output_key)
        write_to_file(output, local_key, self.format, **kwargs)
        self.s3_client.upload_file(local_key, self.bucket_name, output_key)
        if self.available_keys_are_cached:
            if output_key not in self.available_keys:
                self.available_keys.append(output_key)

    def _rm_data(self, key):
        local_key = os.path.join(self.tmp_dir, key)
        if os.path.exists(local_key):
            os.remove(local_key)
        self.s3_client.delete_object(Bucket = self.bucket_name, Key = key)
        if self.available_keys_are_cached:
            if key in self.available_keys:
                self.available_keys.pop(key)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        try:
            self.s3_client.head_object(Bucket = self.bucket_name, Key = data_path)
            return True
        except:
            return False

    @cached_property
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
        
        self.available_keys_are_cached = True
        return files

    def update(self, in_place = False, **kwargs):
        self.options.update({'bucket_name': self.bucket_name, 'tmp_dir': self.tmp_dir})
        new_self = super().update(in_place = in_place, **kwargs)

        if in_place:
            self = new_self
            return self
        else:
            return new_self

    def cleanup(self):
        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

    def get_tile_names_from_file(self, filename: str) -> list[str]:
        local_file = os.path.join(self.tmp_dir, filename)
        os.makedirs(os.path.dirname(local_file), exist_ok = True)
        self.s3_client.download_file(self.bucket_name, filename, local_file)
        return super().get_tile_names_from_file(local_file)