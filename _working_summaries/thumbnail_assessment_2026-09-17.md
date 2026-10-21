# Thumbnail Assessment and Restart Notes

**Date:** 2026-09-17  
**Scope:** `Thumbnail` only

Do not include `DatasetThumbnailManager` or `ThumbnailCollection` in the first implementation pass. Stabilize `Thumbnail` first, then address those separately.

## Relevant Files

- `src/d3tools/thumbnails/thumbnail.py`
- `src/d3tools/thumbnails/colors.py`

No production changes or tests have been made for `Thumbnail` yet. The work so far was analysis and runtime reproduction.

## Current Intended Behavior

`Thumbnail` accepts:

- A raster file path
- An `xarray.DataArray`
- A `geopandas.GeoDataFrame`

It applies a QGIS-style color definition, creates a Matplotlib image, and optionally adds:

- An overlay
- An annotation
- A legend

A color definition currently looks like:

```text
position,r,g,b,a,label
-1,255,0,0,255,low
0,255,255,0,255,normal
1,0,255,0,255,high
inf,0,0,255,255,extreme
```

The intended binning is right-closed:

- First color: values less than or equal to the first break
- Following colors: values greater than the previous break and less than or equal to the current break
- Terminal `inf`: values above the final finite break
- Missing values: separate missing-data category/color

## Confirmed Happy Paths

The current implementation can render:

- An on-disk GeoTIFF with explicit nodata metadata
- A finite two-dimensional `DataArray` when nodata is explicitly set
- Polygon `GeoDataFrame` input in a geographic CRS with a `value` column
- A PNG to a nested destination path

## Reproduced Input Failures

### Raster Without Explicit Nodata

A normal finite `DataArray` with `rio.nodata is None` fails during construction:

```text
TypeError: unsupported operand type(s) for -: 'float' and 'NoneType'
```

Cause: missing-data detection calls `np.isclose(data, None)`.

Expected behavior: finite values remain valid; `NaN` values are missing even if no nodata attribute is set.

### All-Missing Data

All-NaN raster and vector inputs fail during construction:

```text
AttributeError: Thumbnail object has no attribute 'breaks'
```

Cause: the all-missing branch sets `allnan=True` but later code still accesses `self.breaks`.

The current early return in `save()` is therefore never reached.

### Multiband Raster

A true multiband `DataArray` fails later with an unclear rendering error:

```text
ValueError: too many values to unpack (expected 2)
```

Expected behavior:

- Squeeze singleton dimensions.
- Require exactly two dimensions afterward.
- Reject true multiband input with a clear `ValueError` explaining that a single band must be selected.

### Projected Vector Sizing

Vector canvas size is currently derived by dividing coordinate extents by a hardcoded resolution of `0.01`.

Observed equivalent geometries:

- EPSG:4326: shape `(100, 200)`
- EPSG:3857: shape `(11,132,515, 22,263,899)`

This can trigger catastrophic memory allocation.

Expected behavior: use the extent only to derive aspect ratio. Pixel dimensions must be bounded and independent of CRS units.

### Degenerate Geometries

A point or zero-width/zero-height vector extent causes division by zero during image sizing.

Expected behavior: use a square fallback canvas for degenerate extents.

### Vector Value Contract

`Thumbnail` implicitly expects a numeric `value` column but does not validate or document it.

Expected behavior:

- Reject an empty `GeoDataFrame` clearly.
- Require a `value` column.
- Require numeric values, while allowing missing values.
- Support polygons, lines, and points.

## Reproduced Save and Option Failures

### Basename-Only Output

Saving to a filename such as `thumbnail.png` fails because the implementation calls:

```python
os.makedirs("")
```

Expected behavior: create a parent directory only when the parent path is non-empty.

### Annotation `"none"`

`annotation="none"` renders the literal text `none`.

Cause: `.lower` is compared rather than `.lower()` being called, and any string reaches the literal annotation branch first.

### Empty Annotation Dictionary

`annotation={}` without `source_key` or a source filename raises:

```text
UnboundLocalError
```

Cause: annotation text is never assigned.

### Empty Annotation Text

An empty annotation string causes division by zero in automatic font sizing.

Expected behavior: treat an empty string as no annotation, or render it without attempting font-size division. Treating it as disabled is simpler.

### Option Mutation

The annotation dictionary passed by the caller is mutated because `save()` uses `pop()` directly on it.

Expected behavior: copy all option dictionaries before reading or modifying them.

### Repeated Saves

Saving the same `Thumbnail` twice reuses the existing figure and accumulates legends and annotations.

Expected behavior: every `save()` should create an independent figure and close it in `finally`.

### Legend Handling

Current behavior is inconsistent:

- Legend omitted: enabled
- `legend=False` or `None`: disabled
- `legend="none"`: disabled accidentally
- Other arbitrary strings also suppress the legend accidentally

Expected behavior: validate accepted types explicitly.

### All-Missing Output

The intended current behavior appears to be an early return without writing a file. This would leave `thumbnail_file` pointing to a nonexistent path.

Recommended policy: produce a neutral thumbnail displaying `No data`. This gives operational logs and downstream consumers a real file.

This policy should be confirmed before implementation.

## Classification and Color Findings

With a color map ending in `inf`, a finite raster with explicit NaN nodata produced:

```text
breaks = [0, 1, 2]
colormap colors = 3
```

An earlier suspicion that the NaN check was universally off by one is not established. The terminal `inf` entry is excluded from the numeric bins, which affects the sentinel index.

Do not alter bin arithmetic until boundary tests exist.

Current color parser weaknesses:

- Empty or fully malformed files raise a generic tuple-unpacking `ValueError`.
- Invalid lines are silently ignored.
- There is no clear error if no valid entries remain.
- Behavior without a terminal `inf` entry is not explicitly defined.

## Proposed Stable Contract

### Raster Normalization

1. Accept `str` and `xarray.DataArray`.
2. Open raster paths through `rioxarray`.
3. Squeeze singleton dimensions.
4. Require exactly two dimensions afterward.
5. Convert data to a two-dimensional array for classification.
6. Build the missing mask from:
   - `isnull()` / `np.isnan()` where applicable
   - comparison to `rio.nodata` only when nodata is not `None`
7. Allow missing CRS for plain rendering.
8. Require valid source and overlay CRS when reprojection is requested.

### Vector Normalization

1. Require a non-empty `GeoDataFrame`.
2. Require a numeric `value` column.
3. Build missing mask from `value.isna()`.
4. Preserve the input CRS and bounds.
5. Derive only aspect ratio from bounds.
6. Use a square fallback when width or height is zero.
7. Support polygon, line, and point geometries.

### Canvas Sizing

Recommended pixel-oriented behavior:

- Preserve raster or extent aspect ratio.
- Default short side around 900 pixels.
- Cap the long side around 1,800 to 2,400 pixels.
- Do not calculate pixel counts from CRS units.
- Use a square canvas for degenerate extents.

The existing `size`/`dpi` semantics should either be documented and retained or replaced with a clearer pixel-size API. Avoid silently changing them without tests.

### All-Missing Data

Recommended behavior:

- Constructor succeeds with `allnan=True`.
- `save()` writes a neutral background containing `No data`.
- Optional annotation can still be shown.
- Legend is disabled unless explicitly requested.
- `thumbnail_file` refers to an existing file.

### Annotation Options

Recommended behavior:

- Annotation omitted: infer basename from `source_key` or raster filename when available.
- `False`, `None`, or case-insensitive `"none"`: disabled.
- Non-empty string: literal annotation text.
- Dictionary: copy it; optional `text` key overrides inferred text.
- Empty string or dictionary with no inferable text: disabled.
- Unsupported type: raise `TypeError`.

### Legend Options

Recommended behavior:

- Omitted or `True`: enabled.
- `False`, `None`, or case-insensitive `"none"`: disabled.
- Dictionary: copy and pass as Matplotlib legend options.
- Unsupported type/string: raise `TypeError` or `ValueError`.

Recommended default legend policy: show the complete classification so thumbnails remain comparable across dates. A future `used_only=True` option may restrict it to categories present in one image.

This policy should be confirmed before implementation.

### Overlay Options

`Thumbnail` should accept:

- Path as `str`
- `Dataset`
- `GeoDataFrame`

Behavior:

- Copy options before applying defaults.
- Validate source and overlay CRS.
- Reproject overlay to source CRS.
- Preserve original plot extent.
- Raise a clear error when reprojection is impossible.

### Save Lifecycle

Recommended behavior:

1. Parse and copy options.
2. Create a fresh figure.
3. Render data or no-data state.
4. Apply overlay, annotation, and legend.
5. Create parent directory only when needed.
6. Save output.
7. Close figure in `finally`.
8. Return destination path.

Repeated calls must not accumulate artists or depend on a closed figure.

## Proposed Test Matrix

### Constructor and Classification

- Raster path with explicit nodata
- Two-dimensional `DataArray`
- Singleton-band `DataArray`
- Reject true multiband input
- Raster with nodata `None`
- Numeric nodata
- Partial NaN
- All NaN/all nodata
- Polygon vector
- Point vector
- Line vector
- Projected vector
- Empty vector
- Missing `value`
- Non-numeric `value`
- Exact values at every classification boundary
- Missing-value category/color
- Empty/malformed color map
- Color map with terminal `inf`
- Color map without terminal `inf`

### Rendering and Save

- Bounded default dimensions
- Explicit size/DPI behavior
- Nested destination
- Basename-only destination
- Automatic annotation
- Annotation disabled with `False`, `None`, and `"none"`
- Literal annotation
- Dictionary annotation
- Empty annotation
- Default legend
- Legend disabled with `False`, `None`, and `"none"`
- Configured legend
- Overlay from path
- Overlay from `Dataset`
- Overlay from `GeoDataFrame`
- Overlay reprojection
- Missing-CRS error
- Repeated independent saves
- Figure cleanup after success
- Figure cleanup after failure
- All-missing output according to selected policy

## Recommended Restart Sequence

### Slice 1: Input Normalization and Classification

1. Write failing tests for:
   - finite raster with nodata `None`
   - partial NaN
   - all NaN
   - singleton band
   - rejected multiband
   - geographic polygon vector
   - projected vector
   - point vector
   - missing/non-numeric `value`
   - classification boundaries
2. Refactor constructor logic into small raster/vector normalization helpers.
3. Introduce bounded aspect-ratio sizing independent of CRS units.
4. Decide and implement the all-missing policy.
5. Run focused tests.

### Slice 2: Rendering and Save Semantics

1. Add tests for destination handling, annotations, legends, overlays, repeated saves, and cleanup.
2. Make each save create a fresh figure.
3. Normalize option handling without mutating callers.
4. Return the written destination.
5. Run focused and full suites.

After these two slices, address `DatasetThumbnailManager`. Leave `ThumbnailCollection` for a separate step.

## Implementation Plan for Point 4

Goal: stabilize the low-level `Thumbnail` contract before touching `DatasetThumbnailManager` or `ThumbnailCollection`.

### 1. Add a Focused Thumbnail Test Module First

Create `tests/thumbnails/test_thumbnail.py` with small synthetic fixtures:

- Temporary QGIS-style color definition text file.
- Two-dimensional `xarray.DataArray` fixtures with and without CRS/nodata metadata.
- Temporary GeoTIFF fixture written through `rioxarray` for the path-input smoke test.
- `GeoDataFrame` fixtures for polygons, lines, points, missing values, empty frames, missing `value`, non-numeric `value`, projected CRS, and degenerate bounds.

The first test pass should encode existing reproduced failures as expected behavior. This will make the later implementation changes reviewable and prevent accidental color-bin drift.

### 2. Normalize Raster Input in `Thumbnail.__init__`

Change the raster setup to:

- Accept `str` and `xarray.DataArray`.
- Open paths with `rioxarray.open_rasterio()`.
- Squeeze singleton dimensions before storing the render array.
- Require exactly two dimensions after squeezing; raise a clear `ValueError` for true multiband input.
- Compute missing values from `np.isnan()` where supported plus `rio.nodata` comparison only when nodata is not `None`.
- Keep finite rasters valid when `rio.nodata is None`.
- Preserve the current right-closed `np.digitize(..., right=True)` binning until boundary tests prove a change is needed.

Tests to add:

- DataArray without explicit nodata renders and saves.
- NaN cells are assigned to the missing-data class when nodata is absent.
- DataArray with singleton band dimension is accepted.
- DataArray with multiple bands raises `ValueError` naming single-band selection.
- GeoTIFF path input still renders.

### 3. Normalize Vector Input in `Thumbnail.__init__`

Change the vector setup to:

- Reject empty `GeoDataFrame` with `ValueError`.
- Require a `value` column with a clear `ValueError`.
- Require numeric `value` data while allowing missing values.
- Support polygon, line, and point geometries.
- Compute missing values from `value.isna()`.
- Preserve the input CRS and bounds for plotting.

Tests to add:

- Polygon, line, and point frames save a thumbnail.
- Empty frame raises a clear error.
- Missing `value` column raises a clear error.
- Non-numeric `value` column raises a clear error.
- Missing values in `value` use the missing-data class.

### 4. Replace CRS-Unit Canvas Sizing

Move vector shape calculation away from hardcoded `res = 0.01`.

Recommended helper behavior:

- Derive only aspect ratio from bounds.
- Use bounded pixel dimensions independent of CRS units.
- Default short side around 900 pixels.
- Apply a maximum long side, for example 1800 or 2400 pixels, to avoid extreme aspect-ratio allocations.
- Use a square fallback when width or height is zero or invalid.

Tests to add:

- Equivalent EPSG:4326 and EPSG:3857 geometries produce bounded, comparable pixel shapes.
- Degenerate point or zero-width/zero-height extents produce a square shape and save successfully.
- Very wide or tall extents remain under the maximum long-side bound.

### 5. Define and Implement All-Missing Output Policy

Adopt the recommended policy: all-missing raster/vector inputs should write a real neutral thumbnail with `No data` text instead of returning without creating a file.

Implementation notes:

- Ensure constructor initializes all attributes needed by `save()` even for all-missing data.
- `save()` should create a small neutral figure for all-missing data.
- `thumbnail_file` should always point to an existing file after successful `save()`.

Tests to add:

- All-NaN raster saves a file.
- All-missing vector saves a file.
- Saved all-missing image is non-empty and readable by Pillow.

### 6. Make `save()` Stateless and Safer

Change `save()` so every call creates and closes its own figure:

- Always call `make_image()` inside `save()` for non-all-missing data.
- Do not rely on a previously stored `self.fig`, `self.ax`, or `self.im`.
- Close the figure in a `finally` block.
- Create the output directory only when `os.path.dirname(destination)` is non-empty.
- Copy option dictionaries before consuming values.
- Do not mutate caller-provided `annotation`, `legend`, or `overlay` dictionaries.

Tests to add:

- Saving to `thumbnail.png` in the current directory succeeds.
- Calling `save()` twice on the same object produces valid files without accumulating duplicate legend/annotation artists.
- Caller-provided annotation and legend dictionaries are unchanged after save.

### 7. Tighten Annotation, Overlay, and Legend Options

Annotation behavior:

- Treat `False`, `None`, empty string, and case-insensitive `"none"` as disabled.
- For `annotation={}`, use source filename/source key when available; otherwise disable annotation without error.
- Keep literal strings other than `"none"` as annotation text.

Legend behavior:

- Omitted legend means enabled, preserving current default.
- `False`, `None`, and case-insensitive `"none"` mean disabled.
- `dict` means enabled with options.
- Other types/strings should raise a clear `TypeError` or `ValueError` instead of silently suppressing the legend.

Overlay behavior:

- Copy overlay option dictionaries.
- Require a CRS on both source thumbnail data and overlay when reprojection is needed.
- Raise a clear error for unsupported overlay option types.

Tests to add:

- `annotation="none"` and `annotation=""` do not add literal text.
- `annotation={}` does not raise when there is no source key or filename.
- Annotation dictionaries are not mutated.
- `legend="none"` disables legend.
- Arbitrary legend string raises a clear error.
- Legend dictionaries are not mutated.

### 8. Harden Color Parsing Separately

Keep bin arithmetic unchanged, but make parser failures clearer:

- Empty files raise `ValueError` explaining no valid color definitions were found.
- Fully malformed files raise the same clear `ValueError`.
- Decide whether partially malformed files should continue to ignore bad lines or fail fast. For backward compatibility, keep ignoring bad lines for now, but document this behavior.
- Explicitly test behavior with and without terminal `inf`.

Tests to add in `tests/thumbnails/test_colors.py`:

- Valid color file sorts finite breaks and terminal `inf` correctly.
- Empty file raises clear `ValueError`.
- Fully malformed file raises clear `ValueError`.
- Invalid color values raise through `parse_line()`.
- `keep_used_colors()` preserves the current range behavior.

### 9. Release Gate for Point 4

Point 4 is complete when these checks pass with the shared workspace virtualenv:

```bash
/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python -m pytest tests/thumbnails
/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python -m pytest tests/config/test_manager_integration.py tests/config/test_parsers.py
```

The second command is the guard that the low-level `Thumbnail` work did not break thumbnail configuration/manager construction.

### Suggested Work Order

1. Add failing behavior tests for raster/vector normalization and all-missing data.
2. Implement constructor normalization and bounded vector sizing.
3. Run `pytest tests/thumbnails/test_thumbnail.py` and iterate locally.
4. Add save-option tests for basename output, repeated saves, annotations, legends, and option mutation.
5. Refactor `save()` to be stateless and close figures reliably.
6. Add color parser tests and improve parser errors without changing classification semantics.
7. Run the full Point 4 release gate.
8. Only after this passes, move to Point 5: docs and CI coverage for thumbnails, config examples, and release checklist.
