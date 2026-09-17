import os
import stat
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from d3tools.data.datasets.remote_dataset import (
    RemoteDataset,
    S3Dataset,
    SFTPDataset,
    sftp_walk,
)


class FakeRemoteDataset(RemoteDataset):
    """In-memory remote backend exercising RemoteDataset's staging logic."""

    def __init__(self, *, key_pattern, storage=None, **kwargs):
        self.key_pattern = key_pattern
        self.storage = storage if storage is not None else {}
        self.downloads = []
        self.uploads = []
        self.deletions = []
        super().__init__(**kwargs)

    def _download(self, input_key, local_key):
        self.downloads.append((input_key, local_key))
        with open(local_key, "wb") as stream:
            stream.write(self.storage[input_key])

    def _upload(self, local_key, output_key):
        self.uploads.append((local_key, output_key))
        with open(local_key, "rb") as stream:
            self.storage[output_key] = stream.read()

    def _delete(self, key):
        self.deletions.append(key)
        self.storage.pop(key, None)

    def _check_data(self, key):
        return key in self.storage or any(item.startswith(key) for item in self.storage)

    def _walk(self, prefix):
        yield from sorted(key for key in self.storage if key.startswith(prefix))


@pytest.fixture
def fake_remote(tmp_path):
    return FakeRemoteDataset(
        key_pattern="data/{region}/output.txt",
        tmp_dir=str(tmp_path),
        storage={"data/eu/output.txt": b"remote content\n"},
    )


@pytest.fixture
def s3_dataset(tmp_path, monkeypatch):
    client = Mock()
    session = Mock()
    session.client.return_value = client
    monkeypatch.setattr("d3tools.data.datasets.remote_dataset.boto3.Session", Mock(return_value=session))
    monkeypatch.setattr(S3Dataset, "s3_client", None)
    dataset = S3Dataset(
        key_pattern="data/{region}/output.txt",
        bucket_name="test-bucket",
        profile_name="test-profile",
        tmp_dir=str(tmp_path),
    )
    return dataset, client


@pytest.fixture
def sftp_dataset(tmp_path, monkeypatch):
    client = Mock()
    monkeypatch.setattr(SFTPDataset, "sftp_clients", {})
    monkeypatch.setattr(SFTPDataset, "_connect", Mock(return_value=client))
    dataset = SFTPDataset(
        key_pattern="data/{region}/output.txt",
        host="sftp.example.com",
        username="test-user",
        password="test-password",
        tmp_dir=str(tmp_path),
    )
    return dataset, client


class TestRemoteDatasetStaging:
    def test_get_local_key_keeps_remote_hierarchy_inside_tmp_dir(self, fake_remote):
        assert fake_remote.get_local_key("/data/eu/output.txt") == os.path.join(
            fake_remote.tmp_dir,
            "data/eu/output.txt",
        )

    def test_get_data_downloads_once_and_reuses_local_staging_file(self, fake_remote):
        first = fake_remote.get_data(region="eu")
        second = fake_remote.get_data(region="eu")

        assert first == ["remote content\n"]
        assert second == first
        assert [key for key, _ in fake_remote.downloads] == ["data/eu/output.txt"]

    def test_write_data_uploads_staged_file_and_updates_cached_keys(self, fake_remote):
        fake_remote.__dict__["available_keys"] = ["data/eu/existing.txt"]
        fake_remote.available_keys_are_cached = True

        fake_remote.write_data("new content\n", region="africa")

        output_key = "data/africa/output.txt"
        assert fake_remote.storage[output_key] == b"new content\n"
        assert [key for _, key in fake_remote.uploads] == [output_key]
        assert output_key in fake_remote.available_keys

    def test_rm_data_removes_remote_local_and_cached_key(self, fake_remote):
        key = "data/eu/output.txt"
        local_key = fake_remote.get_local_key(key)
        os.makedirs(os.path.dirname(local_key), exist_ok=True)
        with open(local_key, "wb") as stream:
            stream.write(fake_remote.storage[key])
        fake_remote.__dict__["available_keys"] = [key, "data/africa/output.txt"]
        fake_remote.available_keys_are_cached = True

        fake_remote.rm_data(region="eu")

        assert key not in fake_remote.storage
        assert not os.path.exists(local_key)
        assert fake_remote.deletions == [key]
        assert fake_remote.available_keys == ["data/africa/output.txt"]

    def test_get_available_keys_filters_remote_walk_with_key_pattern(self, fake_remote):
        fake_remote.storage.update(
            {
                "data/africa/output.txt": b"africa\n",
                "data/eu/other.txt": b"ignored\n",
            }
        )

        assert fake_remote.get_available_keys() == [
            "data/africa/output.txt",
            "data/eu/output.txt",
        ]

    def test_shapefile_read_downloads_required_sidecars(self, tmp_path):
        base_key = "data/eu/shape"
        storage = {f"{base_key}.{extension}": b"content" for extension in ("shp", "dbf", "shx", "prj")}
        dataset = FakeRemoteDataset(
            key_pattern="data/{region}/shape.shp",
            tmp_dir=str(tmp_path),
            storage=storage,
        )
        expected = object()
        dataset._read_from_file = Mock(return_value=expected)

        result = dataset._read_data(f"{base_key}.shp")

        assert result is expected
        assert [key for key, _ in dataset.downloads] == [
            f"{base_key}.shp",
            f"{base_key}.dbf",
            f"{base_key}.shx",
            f"{base_key}.prj",
        ]

    def test_shapefile_write_uploads_existing_sidecars(self, tmp_path):
        dataset = FakeRemoteDataset(
            key_pattern="data/{region}/shape.shp",
            tmp_dir=str(tmp_path),
        )
        output_key = "data/eu/shape.shp"

        def write_shapefile(_data, local_key, **_kwargs):
            base_key = local_key.removesuffix(".shp")
            os.makedirs(os.path.dirname(local_key), exist_ok=True)
            for extension in ("shp", "dbf", "shx", "prj"):
                with open(f"{base_key}.{extension}", "wb") as stream:
                    stream.write(extension.encode())

        dataset._write_to_file = write_shapefile

        dataset._write_data(object(), output_key)

        assert [key for _, key in dataset.uploads] == [
            "data/eu/shape.shp",
            "data/eu/shape.dbf",
            "data/eu/shape.shx",
            "data/eu/shape.prj",
        ]

    def test_post_update_filters_cached_keys_to_new_pattern(self, tmp_path):
        source = FakeRemoteDataset(
            key_pattern="data/{region}/output.txt",
            tmp_dir=str(tmp_path),
        )
        source.__dict__["available_keys"] = [
            "data/eu/output.txt",
            "data/africa/output.txt",
        ]
        source.available_keys_are_cached = True
        updated = FakeRemoteDataset(
            key_pattern="data/eu/output.txt",
            tmp_dir=str(tmp_path),
        )

        updated._post_update_init(source, update_kwargs={"region": "eu"})

        assert updated.available_keys_are_cached is True
        assert updated.available_keys == ["data/eu/output.txt"]


class TestS3DatasetAdapter:
    def test_download_upload_and_delete_delegate_to_boto_client(self, s3_dataset, tmp_path):
        dataset, client = s3_dataset
        local_key = str(tmp_path / "output.txt")

        dataset._download("remote/input.txt", local_key)
        dataset._upload(local_key, "remote/output.txt")
        dataset._delete("remote/old.txt")

        client.download_file.assert_called_once_with("test-bucket", "remote/input.txt", local_key)
        client.upload_file.assert_called_once_with(local_key, "test-bucket", "remote/output.txt")
        client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="remote/old.txt")

    @pytest.mark.parametrize(
        ("pages", "expected"),
        [
            ([{}], False),
            ([{"Contents": []}], False),
            ([{}, {"Contents": [{"Key": "data/output.txt"}]}], True),
        ],
    )
    def test_check_data_uses_paginated_prefix_search(self, s3_dataset, pages, expected):
        dataset, client = s3_dataset
        paginator = client.get_paginator.return_value
        paginator.paginate.return_value = pages

        assert dataset._check_data("data/") is expected
        client.get_paginator.assert_called_once_with("list_objects_v2")
        paginator.paginate.assert_called_once_with(Bucket="test-bucket", Prefix="data/")

    def test_walk_yields_keys_across_pages(self, s3_dataset):
        dataset, client = s3_dataset
        paginator = client.get_paginator.return_value
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "data/a.txt"}]},
            {},
            {"Contents": [{"Key": "data/b.txt"}, {"Key": "data/c.txt"}]},
        ]

        assert list(dataset._walk("data/")) == [
            "data/a.txt",
            "data/b.txt",
            "data/c.txt",
        ]


class TestSFTPDatasetAdapter:
    def test_download_and_delete_delegate_to_sftp_client(self, sftp_dataset, tmp_path):
        dataset, client = sftp_dataset
        local_key = str(tmp_path / "output.txt")

        dataset._download("remote/input.txt", local_key)
        dataset._delete("remote/old.txt")

        client.get.assert_called_once_with("remote/input.txt", local_key)
        client.remove.assert_called_once_with("remote/old.txt")

    def test_upload_creates_missing_remote_directories(self, sftp_dataset, tmp_path):
        dataset, client = sftp_dataset
        client.stat.side_effect = FileNotFoundError
        local_key = str(tmp_path / "output.txt")

        dataset._upload(local_key, "/root/nested/output.txt")

        assert client.mkdir.call_args_list == [call("/root"), call("/root/nested")]
        client.put.assert_called_once_with(local_key, "/root/nested/output.txt")

    def test_check_data_converts_stat_errors_to_false(self, sftp_dataset):
        dataset, client = sftp_dataset
        client.stat.return_value = object()
        assert dataset._check_data("existing") is True

        client.stat.side_effect = FileNotFoundError
        assert dataset._check_data("missing") is False


def test_sftp_walk_recurses_in_sorted_order():
    client = Mock()
    listings = {
        "/root": [
            SimpleNamespace(filename="z.txt", st_mode=stat.S_IFREG),
            SimpleNamespace(filename="b", st_mode=stat.S_IFDIR),
            SimpleNamespace(filename="a", st_mode=stat.S_IFDIR),
        ],
        "/root/a": [SimpleNamespace(filename="a.txt", st_mode=stat.S_IFREG)],
        "/root/b": [SimpleNamespace(filename="b.txt", st_mode=stat.S_IFREG)],
    }
    client.listdir_attr.side_effect = lambda path: listings[path]

    assert list(sftp_walk(client, "/root")) == [
        ("/root", ["a", "b"], ["z.txt"]),
        ("/root/a", [], ["a.txt"]),
        ("/root/b", [], ["b.txt"]),
    ]