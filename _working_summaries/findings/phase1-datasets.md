# Phase 1 findings: datasets

Checker: `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py docs/configuration/datasets.md docs/datasets`

| # | Doc location | Claim / gap | Evidence | Verdict | Action |
|---|--------------|-------------|----------|---------|--------|
| 1 | `docs/configuration/datasets.md:172` | The statement that SFTP authentication behaviour is not covered by tests is too broad: explicit RSA and Ed25519 key loading and transport authentication are tested. Automatic `~/.ssh` key discovery and password authentication are not exercised by that test. | `src/d3tools/data/datasets/remote_dataset.py:381`; `tests/data/test_remote_dataset.py:261` | wrong | Narrow the test-coverage note to automatic key discovery and password authentication. |
| 2 | `docs/configuration/datasets.md` | The user-configurable `format` option is not described as a dataset option. It may be supplied explicitly; otherwise the factory detects it from the key-pattern extension. The accepted values are material to workflow JSON users. | `src/d3tools/data/datasets/dataset.py:61`; `src/d3tools/data/io_utils.py:32,77` | missing | Add a concise configuration summary and link to Formats and Mixins for the complete format list and processing details. |
| 3 | `docs/configuration/datasets.md` | The user-configurable `tile_names` option is not described on the configuration page. It accepts a list/tuple or a filename containing tile names, and controls availability checks without an explicit tile. | `src/d3tools/data/datasets/dataset.py:91,420`; `src/d3tools/data/data_catalogue.py:748` | missing | Add a concise option note with the accepted forms and a pointer to Dataset Concepts. |
| 4 | `docs/configuration/datasets.md` | The user-configurable `nan_value` option is not described. It is used by raster format processing to convert missing values when reading and writing. | `src/d3tools/data/datasets/dataset.py:94`; `src/d3tools/data/format_mixins/raster_mixin.py:104,173` | missing | Document it as a raster-only option and link to Formats and Mixins. |
| 5 | `docs/datasets/storage-backends.md:67` | The SFTP backend requirements, explicit-key behaviour, automatic filename-filtered key search, and lack of SSH config/agent use match the implementation. | `src/d3tools/data/datasets/remote_dataset.py:345`; `tests/data/test_remote_dataset.py:261` | correct | Keep; no detail change needed beyond the configuration page's test-coverage note. |
| 6 | `docs/datasets/formats-and-mixins.md:37` | The format-to-mixin table, supported format keywords, extension detection, and unsupported-format error agree with the factory and I/O helpers. | `src/d3tools/data/io_utils.py:32,77`; `tests/data/format_mixins/test_raster_mixin.py:83` | correct | Keep. |
| 7 | `docs/datasets/time-and-availability.md:91` | `get_times()` combines local dates with the intersection of dates in all parents and the union of fallback dates; `get_available_keys()` and `get_available_tags()` query the dataset's own storage. | `src/d3tools/data/data_catalogue.py:225` | correct | Keep. |
| 8 | `docs/datasets/concepts.md:71` and `docs/datasets/composition.md:87` | The descriptions of tile names, versions, and update preservation match Dataset and catalogue behavior. | `src/d3tools/data/datasets/dataset.py:243`; `tests/data/test_dataset_versioning.py:15` | correct | Keep. |
| 9 | `docs/configuration/datasets.md:127`; `docs/datasets/storage-backends.md:36` | The `keep_after_reading` default and read-removal/retention semantics match `MemoryDataset`; the statement that read-retention behavior itself is not covered by tests is accurate. Existing tests cover configuration preservation, not the read effect. | `src/d3tools/data/datasets/memory_dataset.py:9`; `tests/data/test_dataset_creation_kwargs.py:114` | correct | Keep. |

## Verdict counts
- correct: 5
- wrong: 1
- missing: 3
- wrong-owner: 0
- unverified: 0
- code-bug?: 0

## code-bug? rows
- None.

## Remaining checker errors
- No errors. One warning remains: `docs/configuration/datasets.md:146` could not be fully verified because DNS resolution failed (`gaierror: [Errno -2] Name or service not known`).