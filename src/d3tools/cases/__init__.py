"""Public API for case generation and layered case traversal.

The cases package exposes the :class:`d3tools.cases.case_manager.CaseManager`
used to build layered option combinations and inspect the resulting case tree.
Individual case objects and helper functions live in the submodules below.
"""

from .case_manager import CaseManager