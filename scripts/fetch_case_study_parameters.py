"""Download the SPI parameter bundle used by the documentation case study.

When publishing a replacement bundle, update ``DOI``, ``ARCHIVE_URL``,
``ARCHIVE_SIZE``, and ``ARCHIVE_SHA256`` together, then update the DOI in the case-study
documentation. Do not commit the downloaded parameter rasters to this repository.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
import tempfile
from pathlib import Path, PurePosixPath
from urllib.request import Request, urlopen
from zipfile import ZipFile

DOI = "https://doi.org/10.5281/zenodo.24556246"
ARCHIVE_URL = (
    "https://zenodo.org/api/records/24556246/files/chirps-spi-parameters-v1.zip/content"
)
ARCHIVE_SIZE = 1_165_891
ARCHIVE_SHA256 = "9b75da63bcb70bfcf1086ed0e1b8dc1a07835092ce6c18d5aa10d15a2f352940"
DEFAULT_DESTINATION = Path(
    "/tmp/d3tools-case-study/parameters/chirps-spi-parameters-v1"
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Download and extract the SPI parameters from {DOI}.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=DEFAULT_DESTINATION,
        help="Directory that will directly contain the 1m and 3m folders",
    )
    return parser.parse_args()


def bundle_is_complete(path: Path) -> bool:
    return (
        all((path / window).is_dir() for window in ("1m", "3m"))
        and len(list(path.glob("*/*/*.tif"))) == 288
    )


def download_archive(path: Path) -> None:
    request = Request(ARCHIVE_URL, headers={"User-Agent": "d3tools-case-study/2.4"})
    with urlopen(request, timeout=120) as response, path.open("wb") as output:
        shutil.copyfileobj(response, output)

    size = path.stat().st_size
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if size != ARCHIVE_SIZE or digest != ARCHIVE_SHA256:
        raise RuntimeError(
            "Downloaded archive failed integrity verification: "
            f"size={size}, sha256={digest}"
        )


def extract_archive(archive_path: Path, destination: Path) -> None:
    with ZipFile(archive_path) as archive:
        for member in archive.infolist():
            member_path = PurePosixPath(member.filename)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise RuntimeError(f"Unsafe path in archive: {member.filename}")
        archive.extractall(destination)


def main() -> int:
    args = parse_arguments()
    destination = args.destination.expanduser().resolve()

    if bundle_is_complete(destination):
        print(f"Parameters already available at {destination}")
        return 0
    if destination.exists():
        raise SystemExit(f"Destination exists but is incomplete: {destination}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".chirps-spi-parameters-", dir=destination.parent
    ) as tmp:
        workspace = Path(tmp)
        archive_path = workspace / "chirps-spi-parameters-v1.zip"
        extracted = workspace / "bundle"
        extracted.mkdir()

        print(f"Downloading parameters from {DOI} ...")
        download_archive(archive_path)
        extract_archive(archive_path, extracted)
        if not bundle_is_complete(extracted):
            raise RuntimeError(
                "Extracted archive does not contain the expected 288 files"
            )
        extracted.rename(destination)

    print(f"Parameters extracted to {destination}")
    print(f"Set SPI_PARAMETER_ROOT={destination} when using a custom destination.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
