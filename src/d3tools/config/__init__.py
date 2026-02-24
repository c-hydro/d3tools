from .workflow_definition import *
from .parsers import dataset_from_config, workflow_from_config
from .workflow_section import WORKFLOW_SECTION_ALIASES, WorkflowSection
from .parsing_pipeline import (
    resolve_env,
    resolve_tags,
    build_datasets,
    resolve_dataset_refs,
    collect_workflow_sections,
    parse_options,
)

import sys
# Legacy module paths -> new modules workflow definition used to be options
from . import workflow_definition as _workflow_definition_mod
sys.modules.setdefault(__name__ + ".options", _workflow_definition_mod)

__all__ = [
    'WORKFLOW_SECTION_ALIASES',
    'WorkflowSection',
    'WorkflowDefinition',
    'Options',
    'dataset_from_config',
    'workflow_from_config',
    'resolve_env',
    'resolve_tags',
    'build_datasets',
    'resolve_dataset_refs',
    'collect_workflow_sections',
    'parse_options',
]
