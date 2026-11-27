"""
Tests for TemplateManager time-dimension handling.

Covers:
- set() storing correct metadata for daily and monthly time coords
- build_array() reconstructing time coords with correct length and start
- Monthly steps across year/month boundaries (28/30/31-day months)
- Daily steps, single-element time axis
- Round-trip: set() from DataArray then build_array() reproduces original coords
- Cache serialization of time-aware templates
"""
import datetime as dt
import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pyproj import CRS

from d3tools.spatial.template_manager import TemplateManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spatial_da(times, step_name="time"):
    """Create a minimal spatially-aware DataArray with a time dimension."""
    n = len(times)
    data = np.ones((n, 4, 5))
    x = np.linspace(10.0, 14.0, 5)
    y = np.linspace(45.0, 42.0, 4)
    da = xr.DataArray(
        data,
        coords={step_name: times, "y": y, "x": x},
        dims=[step_name, "y", "x"],
        attrs={"_FillValue": -9999.0},
    )
    da = da.rio.write_crs(4326)
    da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
    return da


def _daily_dates(start_str, n):
    start = dt.datetime.strptime(start_str, "%Y-%m-%d")
    return [start + dt.timedelta(days=i) for i in range(n)]


def _monthly_dates(start_str, n):
    start = dt.datetime.strptime(start_str, "%Y-%m-%d")
    result = []
    y, m = start.year, start.month
    for _ in range(n):
        result.append(dt.datetime(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def daily_da():
    """5-step daily DataArray starting 2024-01-01."""
    return _make_spatial_da(_daily_dates("2024-01-01", 5))


@pytest.fixture
def monthly_da():
    """6-step monthly DataArray covering two months with different lengths."""
    return _make_spatial_da(_monthly_dates("2023-12-01", 6))


@pytest.fixture
def cross_year_monthly_da():
    """Monthly DataArray crossing year boundary (Nov–Mar)."""
    return _make_spatial_da(_monthly_dates("2023-11-01", 5))


@pytest.fixture
def feb_monthly_da():
    """Monthly DataArray that includes Feb (28-day month)."""
    return _make_spatial_da(_monthly_dates("2024-01-01", 3))  # Jan, Feb, Mar 2024


# ---------------------------------------------------------------------------
# set() – metadata extraction for time dimensions
# ---------------------------------------------------------------------------

class TestSetTimeDimension:

    def test_set_daily_stores_step_d(self, daily_da):
        mgr = TemplateManager()
        mgr.set(daily_da)
        t = mgr.get()
        assert t["dims_steps"]["time"] == "d"

    def test_set_monthly_stores_step_m(self, monthly_da):
        mgr = TemplateManager()
        mgr.set(monthly_da)
        t = mgr.get()
        assert t["dims_steps"]["time"] == "m"

    def test_set_daily_stores_correct_start(self, daily_da):
        mgr = TemplateManager()
        mgr.set(daily_da)
        t = mgr.get()
        start = dt.datetime.fromisoformat(t["dims_starts"]["time"])
        assert start == dt.datetime(2024, 1, 1)

    def test_set_daily_stores_correct_end(self, daily_da):
        mgr = TemplateManager()
        mgr.set(daily_da)
        t = mgr.get()
        end = dt.datetime.fromisoformat(t["dims_ends"]["time"])
        assert end == dt.datetime(2024, 1, 5)

    def test_set_monthly_stores_correct_start(self, monthly_da):
        mgr = TemplateManager()
        mgr.set(monthly_da)
        t = mgr.get()
        start = dt.datetime.fromisoformat(t["dims_starts"]["time"])
        assert start == dt.datetime(2023, 12, 1)

    def test_set_daily_stores_correct_length(self, daily_da):
        mgr = TemplateManager()
        mgr.set(daily_da)
        t = mgr.get()
        assert t["dims_lengths"]["time"] == 5

    def test_set_monthly_stores_correct_length(self, monthly_da):
        mgr = TemplateManager()
        mgr.set(monthly_da)
        t = mgr.get()
        assert t["dims_lengths"]["time"] == 6

    def test_set_single_time_step_defaults_to_d(self):
        """Single-element time axis cannot estimate step; should default to 'd'."""
        da = _make_spatial_da([pd.Timestamp("2024-06-01")])
        mgr = TemplateManager()
        mgr.set(da)
        t = mgr.get()
        assert t["dims_steps"]["time"] == "d"
        assert t["dims_lengths"]["time"] == 1


# ---------------------------------------------------------------------------
# build_array() – coordinate reconstruction for time dimensions
# ---------------------------------------------------------------------------

class TestBuildArrayTimeDimension:

    def test_daily_reconstructed_length_matches_template(self, daily_da):
        mgr = TemplateManager()
        mgr.set(daily_da)
        arr = TemplateManager.build_array(mgr.get())
        assert len(arr["time"]) == 5

    def test_monthly_reconstructed_length_matches_template(self, monthly_da):
        mgr = TemplateManager()
        mgr.set(monthly_da)
        arr = TemplateManager.build_array(mgr.get())
        assert len(arr["time"]) == 6

    def test_daily_first_coord_matches_start(self, daily_da):
        """First reconstructed coord must equal the original first date."""
        mgr = TemplateManager()
        mgr.set(daily_da)
        arr = TemplateManager.build_array(mgr.get())
        first = pd.Timestamp(arr["time"].values[0]).to_pydatetime().replace(tzinfo=None)
        assert first == dt.datetime(2024, 1, 1), (
            f"First time coord {first} != 2024-01-01"
        )

    def test_daily_last_coord_matches_end(self, daily_da):
        """Last reconstructed coord must equal the original last date."""
        mgr = TemplateManager()
        mgr.set(daily_da)
        arr = TemplateManager.build_array(mgr.get())
        last = pd.Timestamp(arr["time"].values[-1]).to_pydatetime().replace(tzinfo=None)
        assert last == dt.datetime(2024, 1, 5)

    def test_monthly_first_coord_matches_start(self, monthly_da):
        mgr = TemplateManager()
        mgr.set(monthly_da)
        arr = TemplateManager.build_array(mgr.get())
        first = pd.Timestamp(arr["time"].values[0]).to_pydatetime().replace(tzinfo=None)
        assert first == dt.datetime(2023, 12, 1), (
            f"First time coord {first} != 2023-12-01"
        )

    def test_cross_year_monthly_correct_sequence(self, cross_year_monthly_da):
        """Monthly coords crossing year boundary should step correctly."""
        mgr = TemplateManager()
        mgr.set(cross_year_monthly_da)
        arr = TemplateManager.build_array(mgr.get())
        times = [pd.Timestamp(v).to_pydatetime().replace(tzinfo=None) for v in arr["time"].values]
        expected = _monthly_dates("2023-11-01", 5)
        assert len(times) == 5
        for got, exp in zip(times, expected):
            assert got == exp, f"Got {got}, expected {exp}"

    def test_feb_monthly_correct_length(self, feb_monthly_da):
        """Monthly steps including Feb should produce exactly 3 values, not drift."""
        mgr = TemplateManager()
        mgr.set(feb_monthly_da)
        arr = TemplateManager.build_array(mgr.get())
        assert len(arr["time"]) == 3

    def test_feb_monthly_months_are_jan_feb_mar(self, feb_monthly_da):
        """Monthly step including Feb: months should be Jan=1, Feb=2, Mar=3."""
        mgr = TemplateManager()
        mgr.set(feb_monthly_da)
        arr = TemplateManager.build_array(mgr.get())
        months = [pd.Timestamp(v).month for v in arr["time"].values]
        assert months == [1, 2, 3]


# ---------------------------------------------------------------------------
# Round-trip: set() then build_array() reproduces original coordinates
# ---------------------------------------------------------------------------

class TestRoundTripTimeDimension:

    def test_daily_roundtrip_coords(self, daily_da):
        original_times = daily_da["time"].values
        mgr = TemplateManager()
        mgr.set(daily_da)
        rebuilt = TemplateManager.build_array(mgr.get())
        np.testing.assert_array_equal(
            rebuilt["time"].values,
            original_times,
            err_msg="Daily time coordinates not preserved in round-trip",
        )

    def test_monthly_roundtrip_coords(self, monthly_da):
        original_times = [pd.Timestamp(v).to_pydatetime().replace(tzinfo=None)
                          for v in monthly_da["time"].values]
        mgr = TemplateManager()
        mgr.set(monthly_da)
        rebuilt = TemplateManager.build_array(mgr.get())
        assert len(rebuilt["time"]) == len(original_times)
        for got, exp in zip(rebuilt["time"].values, original_times):
            assert pd.Timestamp(got).to_pydatetime().replace(tzinfo=None) == exp, (
                f"Monthly round-trip mismatch: got {got}, expected {exp}"
            )

    def test_roundtrip_spatial_coords_unaffected(self, daily_da):
        """Spatial coordinates must not be distorted by time-template round-trip."""
        mgr = TemplateManager()
        mgr.set(daily_da)
        rebuilt = TemplateManager.build_array(mgr.get())
        np.testing.assert_allclose(
            rebuilt["x"].values, daily_da["x"].values, rtol=1e-6
        )
        np.testing.assert_allclose(
            rebuilt["y"].values, daily_da["y"].values, rtol=1e-6
        )


# ---------------------------------------------------------------------------
# Cache serialization – time-aware templates must survive JSON round-trip
# ---------------------------------------------------------------------------

class TestCacheTimeDimension:

    def test_daily_template_cache_file_created(self, daily_da):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = TemplateManager(cache_dir=tmpdir)
            mgr.set(daily_da, spatial_key="tile1")
            cache_file = Path(tmpdir) / "template_tile1.json"
            assert cache_file.exists(), "Cache file not created for time-aware template"

    def test_daily_template_cache_is_valid_json(self, daily_da):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = TemplateManager(cache_dir=tmpdir)
            mgr.set(daily_da, spatial_key="tile1")
            cache_file = Path(tmpdir) / "template_tile1.json"
            with open(cache_file) as f:
                data = json.load(f)
            assert "dims_starts" in data
            assert "dims_steps" in data

    def test_daily_template_reloaded_from_cache(self, daily_da):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr1 = TemplateManager(cache_dir=tmpdir)
            mgr1.set(daily_da, spatial_key="tile1")

            mgr2 = TemplateManager(cache_dir=tmpdir)
            loaded = mgr2.get("tile1", load_from_cache=True)
            assert loaded is not None
            assert loaded["dims_steps"]["time"] == "d"
            assert loaded["dims_lengths"]["time"] == 5

    def test_monthly_template_reloaded_from_cache(self, monthly_da):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr1 = TemplateManager(cache_dir=tmpdir)
            mgr1.set(monthly_da, spatial_key="tile1")

            mgr2 = TemplateManager(cache_dir=tmpdir)
            loaded = mgr2.get("tile1", load_from_cache=True)
            assert loaded is not None
            assert loaded["dims_steps"]["time"] == "m"
            assert loaded["dims_lengths"]["time"] == 6

    def test_reloaded_template_can_build_array(self, daily_da):
        """After loading from cache the template must be usable in build_array."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr1 = TemplateManager(cache_dir=tmpdir)
            mgr1.set(daily_da, spatial_key="tile1")

            mgr2 = TemplateManager(cache_dir=tmpdir)
            loaded = mgr2.get("tile1", load_from_cache=True)
            arr = TemplateManager.build_array(loaded)
            assert len(arr["time"]) == 5
