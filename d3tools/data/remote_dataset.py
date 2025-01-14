import paramiko.ed25519key
import xarray as xr
import pandas as pd
import tempfile
from typing import Optional
import boto3
import os
from functools import cached_property
import shutil
import paramiko

import stat
import posixpath

from .dataset import Dataset
from .io_utils import write_to_file, read_from_file
from ..parse import extract_date_and_tags
from ..exit import register

class RemoteDataset(Dataset):
    type = 'remote'

    def __init__(self, tmp_dir: Optional[str] = None, **kwargs):

        if tmp_dir:
            os.makedirs(tmp_dir, exist_ok = True)
            self.tmp_dir = tmp_dir
        else:
            self.tmp_dir = tempfile.mkdtemp()

        self._creation_kwargs.update({'tmp_dir': self.tmp_dir})

        super().__init__(**kwargs)
        register(self.cleanup)

        self.available_keys_are_cached = False

    @property
    def key_pattern(self):
        return self._key_pattern

    @key_pattern.setter
    def key_pattern(self, key_pattern):
        self._key_pattern = key_pattern

    def _read_data(self, input_key):
        local_key = self.get_local_key(input_key)
        if not os.path.exists(local_key):
            os.makedirs(os.path.dirname(local_key), exist_ok = True)
            self._download(input_key, local_key)

        # if this is a shapefile, also copy the dbf, shx, and prj files
        if self.format == 'shp':
            for ext in ['dbf', 'shx', 'prj']:
                local_ext = self.get_local_key(f"{input_key.replace('.shp', '')}.{ext}")
                self._download(input_key.replace('.shp', f'.{ext}'), local_ext)

        return read_from_file(local_key, self.format)

    def _write_data(self, output: xr.DataArray|pd.DataFrame, output_key: str, **kwargs):
        local_key = self.get_local_key(output_key)
        write_to_file(output, local_key, self.format, **kwargs)
        self._upload(local_key, output_key)

        # If the format is 'shp', also upload the associated files
        if self.format == 'shp':
            base_key = output_key.replace('.shp', '')
            for ext in ['dbf', 'shx', 'prj']:
                local_ext = self.get_local_key(f"{base_key}.{ext}")
                if os.path.exists(local_ext):
                    self._upload(local_ext, f"{base_key}.{ext}")

        if self.available_keys_are_cached:
            if output_key not in self.available_keys:
                self.available_keys.append(output_key)

    def _rm_data(self, key):
        local_key = self.get_local_key(key)
        if os.path.exists(local_key):
            os.remove(local_key)
        self._delete(key)
        if self.available_keys_are_cached:
            if key in self.available_keys:
                self.available_keys.pop(key)

    def _download(self, input_key, local_key):
        raise NotImplementedError

    def _upload(self, local_key, output_key):
        raise NotImplementedError

    def _delete(self, key):
        raise NotImplementedError

    def get_tile_names_from_file(self, filename: str) -> list[str]:
        local_file = self.get_local_key(filename)
        os.makedirs(os.path.dirname(local_file), exist_ok = True)
        self._download(filename, local_file)
        return super().get_tile_names_from_file(local_file)

    def cleanup(self):
        if os.path.exists(self.tmp_dir):
            try:
                shutil.rmtree(self.tmp_dir)
            except:
                print(f"Failed to remove temporary directory: {self.tmp_dir}")

    def get_local_key(self, key):
        if key.startswith('/'):
            key = key[1:]
        return os.path.join(self.tmp_dir, key)

    def update(self, in_place = False, **kwargs):
        new_self = super().update(in_place = in_place, **kwargs)
        if self.available_keys_are_cached:
            new_self.available_keys = []
            for key in self.available_keys:
                try:
                    extract_date_and_tags(key, new_self.key_pattern)
                    new_self.available_keys.append(key)
                except ValueError:
                    pass

            new_self.available_keys_are_cached = True

        if in_place:
            self = new_self
            return self
        else:
            return new_self

    @cached_property
    def available_keys(self):
        self.available_keys_are_cached = True
        return self.get_available_keys()

class S3Dataset(RemoteDataset):
    type = 's3'

    s3_client = None  # Class-level attribute to store the S3 client

    def __init__(self,
                 key_pattern: str,
                 bucket_name: str,
                 tmp_dir: Optional[str] = None,
                 **kwargs):

        self.key_pattern = key_pattern
        self.bucket_name = bucket_name

        # Initialize the S3 client if it hasn't been initialized yet
        if S3Dataset.s3_client is None:
            S3Dataset.s3_client = boto3.client('s3')

        self._creation_kwargs = {'type': self.type, 'bucket_name': self.bucket_name}
        super().__init__(tmp_dir,**kwargs)


    ## INPUT/OUTPUT METHODS
    def _download(self, input_key, local_key):
        S3Dataset.s3_client.download_file(self.bucket_name, input_key, local_key)

    def _upload(self, local_key, output_key):
        S3Dataset.s3_client.upload_file(local_key, self.bucket_name, output_key)

    def _delete(self, key):
        S3Dataset.s3_client.delete_object(Bucket = self.bucket_name, Key = key)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        paginator = S3Dataset.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=data_path):
            if 'Contents' in page and page['Contents']:
                return True
        return False

    def _walk(self, prefix):
        paginator = S3Dataset.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket = self.bucket_name, Prefix = prefix):
            content = page.get('Contents', [])
            for file in content:
                yield file['Key']

    def update(self, in_place = False, **kwargs):
        self.options.update({'bucket_name': self.bucket_name, 'tmp_dir': self.tmp_dir})
        new_self = super().update(in_place = in_place, **kwargs)

        if in_place:
            self = new_self
            return self
        else:
            return new_self

class SFTPDataset(RemoteDataset):
    type = 'sftp'
    sftp_clients = {}  # Class-level attribute to store the SFTP clients

    def __init__(self, key_pattern: str,
                       host: str,
                       username: str,
                       password: Optional[str] = None,
                       port = 22,
                       tmp_dir: Optional[str] = None,
                       **kwargs):

        self.key_pattern = key_pattern

        self.hostname = host
        self.username = username

        self.password = password
        self.port = port
        self.private_key = kwargs.pop('private_key', None)

        self.sftp_client = SFTPDataset.sftp_clients.get(self.hostname)
        if self.sftp_client is None:
            self.sftp_client = self._connect()
            SFTPDataset.sftp_clients[self.hostname] = self.sftp_client

        self._creation_kwargs = {'type': self.type, 'host': self.hostname, 'username': self.username,
                                 'password': self.password, 'private_key': self.private_key,
                                 'port': self.port}

        super().__init__(tmp_dir, **kwargs)

    def _connect(self):

        try:
            if self.password is None:
                if self.private_key is not None:
                    if 'rsa' in self.private_key:
                        private_key = paramiko.RSAKey.from_private_key_file(self.private_key)
                    elif 'ed25519' in self.private_key:
                        private_key = paramiko.ed25519key.Ed25519Key.from_private_key_file(self.private_key)
                    transport = paramiko.Transport((self.hostname, self.port))
                    transport.connect(username=self.username, pkey=private_key)
                else:
                    # get the list of files in '~/.ssh'
                    ssh_files = os.listdir(os.path.expanduser('~/.ssh'))
                    keys = [file for file in ssh_files if 'rsa' in file or 'ed25519' in file and not file.endswith('.pub')]
                    possible_files = [f'~/.ssh/{key}' for key in keys]
                    for file in possible_files:
                        try:
                            if 'rsa' in file:
                                private_key = paramiko.RSAKey.from_private_key_file(os.path.expanduser(file))
                            elif 'ed25519' in file:
                                private_key = paramiko.ed25519key.Ed25519Key.from_private_key_file(os.path.expanduser(file))
                            transport = paramiko.Transport((self.hostname, self.port))
                            transport.connect(username=self.username, pkey=private_key)
                            break
                        except:
                            pass
                    else:
                        raise ValueError("Could not resolve private key")
            else:
                password = os.getenv(self.password.replace('$', '')) if self.password.startswith('$') else self.password
                if password is None:
                    raise ValueError("Could not resolve password")
                transport = paramiko.Transport((self.hostname, self.port))
                transport.connect(username=self.username, password=password)

            self.sftp_client = paramiko.SFTPClient.from_transport(transport)
            return self.sftp_client

        except paramiko.SSHException as e:
            print(f"SSHException: {e}")
            raise
        except Exception as e:
            print(f"Exception: {e}")
            raise

    def _download(self, input_key, local_key):
        self.sftp_client.get(input_key, local_key)

    def _upload(self, local_key, output_key):
        # make sure the directory exists
        dirname = os.path.dirname(output_key)
        self._mkdir_recursive(dirname)

        self.sftp_client.put(local_key, output_key)

    def _mkdir_recursive(self, remote_directory):
        if remote_directory.startswith('/'):
            remote_directory = remote_directory[1:]
            current_dir = '/'
        else:
            current_dir = ''
        dirs = remote_directory.split('/')

        for dir in dirs:
            if dir:  # Skip empty parts
                current_dir = os.path.join(current_dir, dir)
                if not self._check_data(current_dir):
                    self.sftp_client.mkdir(current_dir)

    def _delete(self, key):
        self.sftp_client.remove(key)

    def update(self, in_place = False, **kwargs):
        self.options.update({'host': self.hostname, 'username': self.username, 'tmp_dir': self.tmp_dir,
                             'port': self.port, 'private_key': self.private_key, 'password': self.password})
        new_self = super().update(in_place = in_place, **kwargs)
        new_self.sftp_client = self.sftp_client

        if in_place:
            self = new_self
            return self
        else:
            return new_self

    def _check_data(self, data_key) -> bool:
        try:
            self.sftp_client.stat(data_key)
            return True
        except:
            return False

    def _walk(self, prefix):
        for root, dirs, filenames in sftp_walk(self.sftp_client, prefix):
            for file in filenames:
                yield os.path.join(root, file)

    def _write_data(self, output: xr.DataArray | pd.DataFrame, output_key: str, **kwargs):
        local_key = self.get_local_key(output_key)
        write_to_file(output, local_key, self.format, **kwargs)
        self._upload(local_key, output_key)

        # If the format is 'shp', also upload the associated files
        if self.format == 'shp':
            base_key = output_key.replace('.shp', '')
            for ext in ['dbf', 'shx', 'prj']:
                local_ext = self.get_local_key(f"{base_key}.{ext}")
                if os.path.exists(local_ext):
                    self._upload(local_ext, f"{base_key}.{ext}")

        if self.available_keys_are_cached:
            if output_key not in self.available_keys:
                self.available_keys.append(output_key)

def sftp_walk(sftp, remote_path, rev = False):
    """
    Generator that walks the directory tree rooted at remote_path, similar to os.walk.
    Yields a tuple (dirpath, dirnames, filenames).
    """
    path = remote_path
    files = []
    folders = []
    for item in sftp.listdir_attr(path):
        if stat.S_ISDIR(item.st_mode):
            folders.append(item.filename)
        else:
            files.append(item.filename)
    
    folders.sort(reverse=rev)
    files.sort(reverse=rev)
    yield path, folders, files
    for folder in folders:
        new_path = posixpath.join(path, folder)
        for x in sftp_walk(sftp, new_path):
            yield x