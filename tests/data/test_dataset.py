from unittest import mock
import pytest
import xarray as xr

from data.dataset import Dataset
from timestepping import Dekad, TimeRange

class ConcreteDataset(Dataset):
    type = 'test'

    def _write_data(self, output: xr.DataArray, output_path: str):
        pass

    def _read_data(self, input_path:str):
        pass

    def _check_data(self, data_path) -> bool:
        pass

class TestDataset:

    def setup_method(self):
        self.ds1 = ConcreteDataset(path = 'test_path', file = 'test_file')

    def test_dataset_initialization(self):
        assert self.ds1.dir == 'test_path'
        assert self.ds1.file == 'test_file'

    def test_from_options(self):
        self.ds2 = Dataset.from_options({'type': 'test', 'path': 'test_path', 'file': 'test_file'})
        assert isinstance(self.ds2, ConcreteDataset)
        assert self.ds2.__dict__ == self.ds1.__dict__
    
    def test_get_subclass(self):
        assert Dataset.get_subclass('test') == ConcreteDataset
        with mock.patch('data.dataset.Dataset._defaults', {'type': 'test'}):
            assert Dataset.get_subclass(None) == ConcreteDataset
            assert Dataset.get_subclass('test') == ConcreteDataset

    def test_get_type(self):
        assert self.ds1.get_type('test') == 'test'
        assert self.ds1.get_type() == 'test'
        with mock.patch('data.dataset.Dataset._defaults', {'type': 'test3'}):
            assert Dataset.get_type(None) == 'test3'

    def test_time_signature(self):
        assert self.ds1.time_signature == 'end'
        self.ds1.time_signature = 'start'
        assert self.ds1.time_signature == 'start'
        with mock.patch('data.dataset.Dataset._defaults', {'time_signature': 'end+1'}):
            self.ds2 = ConcreteDataset(path = 'test_path', file = 'test_file')
            assert self.ds2.time_signature == 'end+1'

    def test_get_time_signature(self):
        time = Dekad(2024,1)
        assert self.ds1.get_time_signature(time) == time.end
        self.ds1.time_signature = 'start'
        assert self.ds1.get_time_signature(time) == time.start
        self.ds1.time_signature = 'end+1'
        assert self.ds1.get_time_signature(time) == (time+1).start

    def test_get_data(self):
        # Test with check_data = True (data available)
        with mock.patch('data.dataset.Dataset.check_data') as mock_check_data:
            mock_check_data.return_value = True
            with mock.patch(f'{__name__}.ConcreteDataset._read_data') as mock_read_data,\
                 mock.patch(f'data.dataset.straighten_data') as mock_straighten_data,\
                 mock.patch(f'data.dataset.reset_nan') as mock_reset_nan:
                
                self.ds1.get_data(time = Dekad(2024,1))
                mock_read_data.assert_called_once_with('test_path/test_file')

                data = mock_read_data.return_value
                mock_straighten_data.assert_called_once_with(data)

                strainghtened_data = mock_straighten_data.return_value
                mock_reset_nan.assert_called_once_with(strainghtened_data)

                # Test the final bit about the template here:
                with mock.patch('data.dataset.Dataset.make_template_from_data') as mock_make_template_from_data:
                    self.ds1.template = None
                    self.ds1.get_data(time = Dekad(2024,1))

                    nanreset_data = mock_reset_nan.return_value
                    mock_make_template_from_data.assert_called_once_with(nanreset_data)

        # Test with check_data = False (data not available)
        with mock.patch('data.dataset.Dataset.check_data') as mock_check_data:
            mock_check_data.return_value = False

            # this should get an error: no parents
            with pytest.raises(ValueError):
                self.ds1.get_data(time = Dekad(2024,1))

            # this should work
            self.ds1.parents = {'parent1': 'parent1'}
            with mock.patch('data.dataset.Dataset.make_data') as mock_make_data:
                self.ds1.get_data(time = Dekad(2024,1))
                mock_make_data.assert_called_once_with(Dekad(2024,1))

    def test_write_data(self):

        self.ds1.template = xr.DataArray()

        with mock.patch(f'{__name__}.ConcreteDataset._write_data') as mock_write_data,\
             mock.patch(f'data.dataset.Dataset.set_metadata') as mock_set_metadata:
            self.ds1.write_data(None, time = Dekad(2024,1))
            mock_set_metadata.assert_called_once_with(self.ds1.template, Dekad(2024,1), '%Y-%m-%d')

            data_with_metadata = mock_set_metadata.return_value
            mock_write_data.assert_called_once_with(data_with_metadata, 'test_path/test_file')

    def test_make_data(self):
        with pytest.raises(ValueError):
            self.ds1.make_data()

        mock_parent = mock.Mock()
        mock_parent.get_data.return_value = 'parent_data'

        self.ds1.parents = {'parent1': mock_parent}
        self.ds1.fn = lambda parent1: parent1 + '_function_applied'
        with mock.patch('data.dataset.Dataset.write_data') as mock_write_data:
            self.ds1.make_data(Dekad(2024,1))
            mock_write_data.assert_called_once_with('parent_data_function_applied', Dekad(2024,1))

    def test_get_times(self):
        self.ds1.parents = None
        with mock.patch(f'{__name__}.ConcreteDataset.check_data') as mock_check_data:
            time_range = TimeRange('2024-01-01', '2024-01-09')
            ndays = time_range.length('days')
            alternate = [True, False] * (ndays//2) + [True] * (ndays%2)
            mock_check_data.side_effect = alternate
            times = self.ds1.get_times(time_range)
            assert times == [t.end for t in time_range.days][0::2]

    def test_check_data(self):
        with mock.patch(f'{__name__}.ConcreteDataset._check_data') as mock_check_data:
            self.ds1.check_data()
            mock_check_data.assert_called_once_with('test_path/test_file')

    def test_path(self):
        self.ds2 = ConcreteDataset(path = 'test_path_%Y%m%d', file = 'test_file_{test}')
        self.ds2.time_signature = 'start'

        assert self.ds2.path(Dekad(2024,1), test = 'test2') == 'test_path_20240101/test_file_test2'

    def test_get_template(self):
        self.ds1.template = None
        self.ds1.start = 'start'
        with mock.patch(f'{__name__}.ConcreteDataset.get_data') as mock_get_data:
            self.ds1.get_template()
            mock_get_data.assert_called_once_with(time = 'start')

            # test that it doesn't call get_data if the template is already there
            self.ds1.template = 'template'
            assert self.ds1.get_template() == 'template'