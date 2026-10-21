# Dataset Storage Backends

The Dataset API separates storage access from data-format processing. The
storage backend resolves keys and performs reads, writes, discovery, and removal;
format mixins process the data itself.

## Local

`LocalDataset` stores files on the local filesystem. It accepts either:

```json
{
    "type": "local",
    "path": "/data/output/",
    "filename": "result_%Y%m%d.tif"
}
```

or:

```json
{
    "type": "local",
    "key_pattern": "/data/output/result_%Y%m%d.tif"
}
```

`dir` and `file` are also accepted constructor aliases for the directory and
filename components.

"local" is the type Datasets objects default to when their `type` is unspecified.

## Memory

`MemoryDataset` stores data in memory and is useful for tests or intermediate
results. It requires a `key_pattern` and can be configured with
`keep_after_reading`.

```python
from d3tools.data import MemoryDataset

dataset = MemoryDataset(
    key_pattern="memory_{tile}.txt",
    keep_after_reading=True,
)
```

## S3

`S3Dataset` requires `key_pattern` and `bucket_name`. `region_name`,
`profile_name`, and `tmp_dir` are optional. The backend uses boto3 and the
standard AWS credential chain.

```json
{
    "type": "s3",
    "bucket_name": "my-bucket",
    "key_pattern": "data/result_%Y%m%d.tif",
    "region_name": "us-east-1"
}
```

`OVHS3Dataset` extends the S3 backend with a required `endpoint_url`.

## SFTP

`SFTPDataset` requires `key_pattern`, `host`, and `username`. `password` is
optional. When no password is supplied, the implementation can use an explicit
RSA or Ed25519 `private_key`, or search `~/.ssh` for matching private-key
filenames. It does not automatically use SSH configuration or an SSH agent.

See the SFTP section in [Dataset Configuration](../configuration/datasets.md#sftp)
for authentication examples.

## Remote Caching

Remote datasets download data into a temporary local directory before format
processing and upload written data back to the remote backend. `tmp_dir` can be
used to choose the temporary directory. Remote datasets also share cached
connections and preserve the connection when updated or copied.

## Backend Contract

Each backend implements the storage operations needed by Dataset:

- read and write a concrete key;
- remove a key;
- check whether a key or prefix exists;
- walk a prefix for catalogue discovery.

Application code should normally use the public Dataset methods rather than
calling backend methods directly.
