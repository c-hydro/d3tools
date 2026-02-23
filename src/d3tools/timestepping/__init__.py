from .time_utils import *
from .timewindow import *

from .timeperiods import *
from .time_parsing import *

from .timeperiods import timestep as _timestep_mod
from .timeperiods import fixed_num_timestep as _fixed_num_mod
from .timeperiods import fixed_doy_timestep as _fixed_doy_mod
from .timeperiods import fixed_len_timestep as _fixed_len_mod

import sys

# Legacy module paths -> new modules
sys.modules.setdefault(__name__ + ".timestep", _timestep_mod)
sys.modules.setdefault(__name__ + ".fixed_num_timestep", _fixed_num_mod)
sys.modules.setdefault(__name__ + ".fixed_doy_timestep", _fixed_doy_mod)
sys.modules.setdefault(__name__ + ".fixed_len_timestep", _fixed_len_mod)