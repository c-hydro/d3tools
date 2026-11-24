# CHIRPS-to-SPI case study

## Case study overview and prerequisites

This walk-through connects three engines:

1. DOOR downloads 10-day CHIRPS precipitation for a fixed area.
2. DAM creates 1-month and 3-month accumulations while retaining a 10-day
   output cadence.
3. DRYES computes SPI1 and SPI3 from pre-computed parameters.

Install d3tools and all three engines: [DOOR](https://github.com/c-hydro/door),
[DAM](https://github.com/c-hydro/dam), and
[DRYES](https://github.com/c-hydro/dryes). The source CHIRPS example uses no
credentials. The run needs network access to the CHIRPS service and enough
local disk for raw precipitation, aggregates, indices, parameters, and logs.
The sample bounding box is `[8, 38.5, 10, 41.5]`; the period used below is
2021, with the parameter reference period recorded in the configuration as
1990–2020. The parameter bundle fixes the matching grid as well as the area,
aggregation windows, and reference period; do not substitute a different grid
or resample these inputs. Parameter generation and SPI methodology are owned
by DRYES documentation and are outside this guide.

All outputs use the `output_root` tag, set to `/tmp/d3tools-case-study` for
the documented safe test location. Change that one tag in each file to choose
another writable root. Outputs are organized beneath `input/chirps`,
`aggregate`, `spi`, and `logs`.

The SPI bundle is not available in this documentation checkout. In
`03_spi.json` and the combined workflow, `parameter_root` is therefore an
explicit placeholder. Replace `/path/to/precomputed-spi-parameters` with the
location of the matching bundle before running Step 3. The bundle is expected
to contain the DRYES parameter rasters (including gamma `a`, `loc`, and
`scale`, and `prob0`) for each dekad and both aggregation windows. This
distribution and its provenance remain an open maintainer decision.

## Step 1: download 10-day CHIRPS precipitation (DOOR)

The full file below is the Step 1 workflow. Its d3tools-facing parts are the
root tag, destination dataset, dataset timestep, and run range. The DOOR
section is engine-owned; see [DOOR](https://github.com/c-hydro/door) for its
complete payload contract.

<!-- check-docs: skip -->
```json
--8<-- "docs/workflow_orchestration/case-study/01_download_chirps.json"
```

Run it from a d3tools checkout:

```bash
python scripts/run_workflow.py docs/workflow_orchestration/case-study/01_download_chirps.json \
  --start 2020-10-01 \
  --end 2021-12-31
```

Start at least three months (eight 10-day timesteps) before the first SPI
date so the 3-month accumulation has input history. The downloaded files
appear beneath `<output_root>/input/chirps/`. This example was run
successfully for the 45 dekads from 2020-10-01 through 2021-12-31.

## Step 2: aggregate to 1-month and 3-month windows (DAM)

The full Step 2 workflow reuses the Step 1 input definition and writes
window-templated aggregate keys. The DAM `aggregate_times` block is
engine-owned; see [DAM](https://github.com/c-hydro/dam) for its full contract.

<!-- check-docs: skip -->
```json
--8<-- "docs/workflow_orchestration/case-study/02_aggregate.json"
```

Run the aggregation over the download period, including the warm-up data:

```bash
python scripts/run_workflow.py docs/workflow_orchestration/case-study/02_aggregate.json \
  --start 2020-10-01 \
  --end 2021-12-31
```

The DAM example was run successfully: it wrote 43 1-month and 37 3-month
GeoTIFFs at dekadal dates beneath `<output_root>/aggregate/1m/` and
`<output_root>/aggregate/3m/`. This verifies this example's window/cadence
combination with the installed DAM environment; engine option semantics
remain owned by [DAM](https://github.com/c-hydro/dam).

## Step 3: compute SPI1 and SPI3 (DRYES)

Step 3 reads the 1-month and 3-month aggregate datasets and the matching
pre-computed parameter rasters. Parameter references use update tags for the
parameter name; the parameter key pattern includes an `{agg_window}` slot,
as in the DRYES example. `timesteps_per_year` is set to 36 for the 10-day
timestep. The DRYES section is engine-owned; see
[DRYES](https://github.com/c-hydro/dryes) for its full payload contract.
Confirm with DRYES how its `agg_window` cases bind that path slot before
running both windows; this integration has not been engine-verified.

<!-- check-docs: skip -->
```json
--8<-- "docs/workflow_orchestration/case-study/03_spi.json"
```

After supplying the parameter bundle location, run the target 2021 period:

```bash
python scripts/run_workflow.py docs/workflow_orchestration/case-study/03_spi.json \
  --start 2021-01-01 \
  --end 2021-12-31
```

SPI output keys are written beneath `<output_root>/spi/`. Step 3 is not
currently runnable without the matching parameters. The bundle must match the
fixed area, grid, aggregation windows, dekadal cycle, and 1990–2020 reference
period. How to calculate those parameters belongs to DRYES documentation,
not this guide.

## Run the whole chain as one workflow

The combined JSON keeps the three sections in order and defines shared
datasets and logging once. The full file is:

<!-- check-docs: skip -->
```json
--8<-- "docs/workflow_orchestration/case-study/chirps_spi_case_study.json"
```

After setting `parameter_root` to the required bundle location, run the
combined workflow with the shared download/warm-up range:

```bash
python scripts/run_workflow.py docs/workflow_orchestration/case-study/chirps_spi_case_study.json \
  --start 2020-10-01 \
  --end 2021-12-31
```

A supplied workflow range is passed to every section, so the combined run
uses that interval throughout; run Step 3 separately if only the 2021 SPI
output range is wanted. The sequence is download, aggregate, then SPI, and a
failure stops later sections. Inspect `<output_root>/input/chirps/`,
`aggregate/`, `spi/`, and `logs/`; the log includes a separate structured
run-state file. See [Workflows](workflows.md) for ordering and range rules,
and [Logging](logging.md) for log behavior. The combined workflow has not
been executed: Step 3 is blocked by the unavailable parameters, and the
DRYES parameter-window binding is still an open question.
