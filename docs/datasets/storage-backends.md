# Dataset Storage Backends

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle).

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

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle) for backend behavior and the limitation on read-retention coverage; operator options belong to [Dataset Configuration](../configuration/datasets.md).

## S3

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle) for backend responsibilities; operator options belong to [Dataset Configuration](../configuration/datasets.md).

## SFTP

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle) for the backend boundary; operator options belong to [Dataset Configuration](../configuration/datasets.md).

## Remote Caching

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle).

## Backend Contract

See [Developer Guide — Extend supported dataset seams](../developer/extensions.md#extend-supported-dataset-seams).
