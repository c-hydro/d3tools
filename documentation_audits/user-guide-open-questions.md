# User Guide Open Questions

## Case-study engine confirmation

- **SPI parameter distribution (blocks Step 3):** No pre-computed SPI parameter
  bundle or provenance is available in this checkout. Decide whether the bundle
  belongs in the repository or at a stable external URL, then record its
  generator/DRYES version, area and grid, calibration period, and data
  provenance. Until then the JSON uses
  `/path/to/precomputed-spi-parameters`; Step 3 and the combined workflow
  cannot be run to completion.
- **DRYES parameter selectors:** The case-study file follows the SPI example's
  four parameter-name update tags and `{agg_window}` path slot. Confirm that
  the engine binds that slot to both its 1-month and 3-month cases, and verify
  the exact parameter directory/file layout when the bundle becomes available.
  The outline asks for an aggregation-window update tag as well, while the
  approved `spi_example.json` uses `par_name` updates and leaves
  `{agg_window}` in the parameter key pattern. The guide follows the observed
  source pattern rather than inventing a static selector for a two-window
  engine case; confirm whether the additional d3tools update tag is actually
  required and how it should vary between windows.
- **SPI dekad setting:** The SPI example uses `timesteps_per_year: 12`, while
  other DRYES examples use 36 for dekadal data. The case study sets 36 to match
  its 10-day cadence, but confirm this value for SPI with the engine owner.
- **Opaque engine settings retained from examples:** Confirm the intended
  meanings and valid values of DOOR's `options.get_prelim`, DAM's `pid` and
  `agg_func`, and DRYES's `min_reference` and `zero_threshold` against their
  engine documentation. The guide leaves them engine-owned and does not infer
  universal behavior from the example values.
- **CHIRPS product and redistribution:** The documented 45-dekad live
  download succeeded using the checked-in example's `get_prelim: true`.
  Confirm that this is the intended preliminary/final product choice and
  review usage/redistribution terms before publishing any parameter bundle.

## Additional runnable-example candidates

The approved single case study does not exercise these operator configuration
paths. They currently have focused JSON examples in the guide; a later
maintainer decision may add dedicated runnable examples:

- dataset fallbacks, file versions, and explicit tile lists;
- memory datasets and S3-compatible/SFTP locations;
- dataset thumbnails and format-dependent dataset logs;
- `times_from_run`, `all_available`, `repeat_window`, and `split` execution
  options.

Do not infer universal engine behavior from one checked-in engine example.
Complete payload rules remain owned by DOOR, DAM, and DRYES.
