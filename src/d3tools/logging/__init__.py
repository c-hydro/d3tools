"""
Logging module for d3tools.

This module handles:
1. Dataset logging - tracking data written to disk (DatasetLogManager)
2. Workflow logging - aggregated logging across workflow execution (WorkflowLogManager)
3. Shared utilities - formatters, handlers, and helper functions

The logging functionality is separated from core functionality to enable:
- Workflow-level logging (not just dataset-level)
- Centralized logging configuration
- Cross-module logging (d3tools, door, dam, dryes)
- Easier decoupling of observability from data operations
"""

from .dataset_log_manager import DatasetLogManager
from .workflow_log_manager import WorkflowLogManager

__all__ = ['DatasetLogManager', 'WorkflowLogManager']
