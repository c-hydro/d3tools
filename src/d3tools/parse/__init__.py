"""Public parsing API.

Phase-1 note:
- Keep legacy exports via ``parse_utils`` for backward compatibility.
- Expose new scaffold modules so callers can migrate progressively.
"""

from .parse_utils import *

from .string_rendering import substitute_string, substitute_values
from .config_resolution import set_env, set_dataset
from .key_parser import ParsedKey, KeyParser
