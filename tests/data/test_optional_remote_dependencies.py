"""Verify that remote backends do not leak dependencies into base imports."""

import subprocess
import sys
import textwrap


def test_base_package_imports_without_remote_dependencies():
    script = textwrap.dedent(
        """
        import importlib.abc
        import sys

        class BlockRemoteDependencies(importlib.abc.MetaPathFinder):
            blocked = {"boto3", "botocore", "paramiko", "requests"}

            def find_spec(self, fullname, path=None, target=None):
                if fullname.partition(".")[0] in self.blocked:
                    raise ModuleNotFoundError(
                        f"blocked optional dependency: {fullname}",
                        name=fullname,
                    )
                return None

        sys.meta_path.insert(0, BlockRemoteDependencies())

        import d3tools
        from d3tools.data import (
            Dataset,
            LocalDataset,
            MemoryDataset,
            RemoteDataset,
            S3Dataset,
            SFTPDataset,
        )

        local = LocalDataset(key_pattern="/tmp/example.txt")
        memory = MemoryDataset(key_pattern="example.txt")
        assert isinstance(local, Dataset)
        assert isinstance(memory, Dataset)
        assert issubclass(S3Dataset, RemoteDataset)
        assert issubclass(SFTPDataset, RemoteDataset)

        for backend, kwargs, expected in (
            (
                S3Dataset,
                {"key_pattern": "example.txt", "bucket_name": "example"},
                "S3 dataset support requires the 'remote-data' extra",
            ),
            (
                SFTPDataset,
                {
                    "key_pattern": "example.txt",
                    "host": "example.com",
                    "username": "example",
                },
                "SFTP dataset support requires the 'remote-data' extra",
            ),
        ):
            try:
                backend(**kwargs)
            except ModuleNotFoundError as error:
                assert expected in str(error)
                assert "cima-d3tools[remote-data]" in str(error)
            else:
                raise AssertionError(f"{backend.__name__} accepted missing dependencies")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
