"""Public configuration API for d3tools workflows.

This package exposes workflow-definition objects, parsing helpers, and the
configuration pipeline used to resolve environment variables, tags, datasets,
and section metadata before execution.
"""

from .workflow_definition import WorkflowDefinition
from .options import Options
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
