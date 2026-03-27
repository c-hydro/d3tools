"""
Logging module for d3tools.

This module handles:
1. Dataset logging - tracking data written to disk (DatasetLogManager)
2. Future: Workflow logging - aggregated logging across datasets
3. Future: Application logging - errors, warnings, info messages

The logging functionality is separated from Dataset to enable:
- Workflow-level logging (not just dataset-level)
- Centralized logging configuration
- Easier decoupling of observability from data operations
"""

from .dataset_log_manager import DatasetLogManager

__all__ = ['DatasetLogManager']
