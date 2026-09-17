import importlib

import pytest


@pytest.mark.parametrize(
    ("legacy_name", "current_name", "symbol"),
    [
        ("d3tools.data.dataset", "d3tools.data.datasets.dataset", "Dataset"),
        ("d3tools.data.local_dataset", "d3tools.data.datasets.local_dataset", "LocalDataset"),
        ("d3tools.data.memory_dataset", "d3tools.data.datasets.memory_dataset", "MemoryDataset"),
        ("d3tools.data.remote_dataset", "d3tools.data.datasets.remote_dataset", "RemoteDataset"),
        (
            "d3tools.timestepping.timestep",
            "d3tools.timestepping.timeperiods.timestep",
            "TimeStep",
        ),
        (
            "d3tools.timestepping.fixed_num_timestep",
            "d3tools.timestepping.timeperiods.fixed_num_timestep",
            "FixedNTimeStep",
        ),
        (
            "d3tools.timestepping.fixed_doy_timestep",
            "d3tools.timestepping.timeperiods.fixed_doy_timestep",
            "FixedDOYTimeStep",
        ),
        (
            "d3tools.timestepping.fixed_len_timestep",
            "d3tools.timestepping.timeperiods.fixed_len_timestep",
            "FixedLenTimeStep",
        ),
        (
            "d3tools.timestepping.timerange",
            "d3tools.timestepping.timeperiods.timerange",
            "TimeRange",
        ),
        (
            "d3tools.timestepping.timeperiod",
            "d3tools.timestepping.timeperiods.timeperiod",
            "TimePeriod",
        ),
    ],
)
def test_legacy_module_aliases_preserve_module_and_symbol_identity(
    legacy_name,
    current_name,
    symbol,
):
    legacy_module = importlib.import_module(legacy_name)
    current_module = importlib.import_module(current_name)

    assert legacy_module is current_module
    assert getattr(legacy_module, symbol) is getattr(current_module, symbol)


@pytest.mark.parametrize(
    ("package_name", "attribute", "current_name"),
    [
        ("d3tools.data", "dataset", "d3tools.data.datasets.dataset"),
        ("d3tools.data", "local_dataset", "d3tools.data.datasets.local_dataset"),
        ("d3tools.data", "memory_dataset", "d3tools.data.datasets.memory_dataset"),
        ("d3tools.data", "remote_dataset", "d3tools.data.datasets.remote_dataset"),
        (
            "d3tools.timestepping",
            "timestep",
            "d3tools.timestepping.timeperiods.timestep",
        ),
        (
            "d3tools.timestepping",
            "fixed_num_timestep",
            "d3tools.timestepping.timeperiods.fixed_num_timestep",
        ),
        (
            "d3tools.timestepping",
            "fixed_doy_timestep",
            "d3tools.timestepping.timeperiods.fixed_doy_timestep",
        ),
        (
            "d3tools.timestepping",
            "fixed_len_timestep",
            "d3tools.timestepping.timeperiods.fixed_len_timestep",
        ),
        (
            "d3tools.timestepping",
            "timerange",
            "d3tools.timestepping.timeperiods.timerange",
        ),
        (
            "d3tools.timestepping",
            "timeperiod",
            "d3tools.timestepping.timeperiods.timeperiod",
        ),
    ],
)
def test_legacy_modules_are_exposed_as_package_attributes(
    package_name,
    attribute,
    current_name,
):
    package = importlib.import_module(package_name)
    current_module = importlib.import_module(current_name)

    assert getattr(package, attribute) is current_module