# Developer Guide Open Questions

These items remain unresolved in the implementation/test evidence consolidated
for this guide. The narrative deliberately avoids presenting them as supported
contracts.

## Behavior and reliability decisions

- **Dataset placeholder updates:** `set_dataset()` assigns the result of
  `update(**tags)`, while ordinary in-place update methods may return `None`.
  Add a regression test with such an updater and decide whether to retain the
  original object after mutation.
- **Dataset reference matching:** references with trailing text have been
  observed to match even though the supported documented form is a complete
  string placeholder. Decide whether this is supported or should be rejected.
- **Email notification:** validate missing credentials, test sending,
  attachments, and failure paths, and decide what guarantees can be made before
  removing its experimental status.
- **Thumbnail factory:** `DatasetThumbnailManager.from_dict()` applies the
  dataset factory to `colors` unconditionally when a factory is supplied,
  despite its documented conditional conversion. Decide the accepted input
  types and align implementation/tests.
- **Empty thumbnail collection:** determine the expected result/error when PDF
  combination has no eligible images and add coverage.
- **Colour labels containing commas:** decide whether parsing preserves commas
  or the contract should describe the current separator behavior; add a focused
  test.
- **Exit/cleanup semantics:** add direct coverage and establish callback order,
  process-exit timing, exception interruption, and deferred `rm_at_exit`
  behavior. Decide whether `ExitHandler` is a supported package-level API or
  only a submodule API.

## API boundaries and coverage

- Define curated exports for spatial and timestepping APIs; avoid turning
  incidental wildcard-exported imports into public compatibility contracts.
- Make the intended package-level exception exports explicit and add direct
  tests for untested exception attributes and message formatting.
- Add focused parser tests for updater return handling and clarify accepted
  dataset-reference matching.
- Add tests for thumbnail factory conversion, empty collection behavior, and
  comma-containing labels.
- Keep notification experimental until credential and behavior decisions have
  corresponding tests.
