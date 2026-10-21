# Phase 1 findings: timestepping

| # | Doc location | Claim | Evidence | Verdict | Action |
|---|--------------|-------|----------|---------|--------|
| 1 | docs/timestepping.md | The page names the fixed-step base class as `FixedNumTimeStep`, but the exported public class is `FixedNTimeStep`. | `src/d3tools/timestepping/timeperiods/fixed_num_timestep.py:18-27`; `tests/test_legacy_module_aliases.py:7-23` | wrong | rename the class in the page and keep the public name consistent with the package API |
| 2 | docs/timestepping.md | The section "Dataset Timestepping" is about dataset configuration and key rendering, not the core timestepping module API. | `src/d3tools/data/datasets/dataset.py:66-79`; `src/d3tools/data/data_catalogue.py:53-170`; `docs/datasets/time-and-availability.md:1-107` | wrong-tier | replace with a short link to the owning dataset guide |
| 3 | docs/timestepping.md | The section "Workflow Ranges" belongs to workflow-section execution logic rather than the timestepping module page. | `src/d3tools/config/workflow_definition.py:120-176`; `src/d3tools/config/workflow_section.py:114-188`; `docs/configuration/workflow-sections.md:1-220` | wrong-tier | replace with a short link to the owning workflow guide |
| 4 | docs/timestepping.md | `TimeStep.from_unit()` normalizes unit strings and accepts the documented aliases such as `d`/`daily`, `m`/`monthly`, `t`/`dekads`, `y`/`annual`, and `v`/`viirs`. | `src/d3tools/timestepping/time_utils.py:237-318`; `src/d3tools/timestepping/timeperiods/timestep.py:38-76` | correct | keep |
| 5 | docs/timestepping.md | `TimeWindow.from_str()` parses relative durations such as `3 months`, `1-year`, and `8d`/`10d`-style inputs with separator rules. | `src/d3tools/timestepping/timewindow.py:56-99`; `tests/timestepping/test_timewindow.py:28-80` | correct | keep |
| 6 | docs/timestepping.md | The page should call out the primary public timestepping operations (`TimeRange.get_timesteps*`, `TimeWindow.apply`, `find_unit_of_time`) instead of dataset/workflow config material. | `src/d3tools/timestepping/time_utils.py:237-318`; `src/d3tools/timestepping/timewindow.py:102-174`; `src/d3tools/timestepping/timeperiods/timerange.py:90-174` | missing | add a short list of the core public APIs and keep only cross-links for dataset/workflow topics |

## Verdict counts
- correct: 2
- wrong: 1
- missing: 1
- wrong-tier: 2
- unverified: 0
- code-bug?: 0

## code-bug? rows
- none

## remaining checker errors
- none
