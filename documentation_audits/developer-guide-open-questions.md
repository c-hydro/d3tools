# Developer Guide Open Questions

The scope decision below is settled. The remaining items are unresolved in the
implementation/test evidence consolidated for this guide, so the narrative
deliberately avoids presenting them as supported contracts.

## Resolved scope decision

- **Email notification:** keep it experimental and unsupported for current use.
  Do not recommend it or present it as a supported workflow feature. No
  reliability work is planned.

## Behavior and reliability decisions

- **Exit/cleanup semantics:** add direct coverage and establish callback order,
  process-exit timing, exception interruption, and deferred `rm_at_exit`
  behavior. Decide whether `ExitHandler` is a supported package-level API or
  only a submodule API.

## API boundaries and coverage

- Define curated exports for spatial and timestepping APIs; avoid turning
  incidental wildcard-exported imports into public compatibility contracts.
- Make the intended package-level exception exports explicit and add direct
  tests for untested exception attributes and message formatting.
