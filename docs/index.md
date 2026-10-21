# d3tools Documentation

**d3tools** is a workflow orchestration framework that provides general-purpose tools shared between the DOOR (downloaders), DAM (data processing), and DRYES (drought indices) repositories.

## Quick Links

- [Getting Started](getting-started.md) - Installation and first workflow
- [Configuration Guide](configuration/overview.md) - JSON configuration format
- [Logging Guide](logging/overview.md) - Workflow logging setup
- [Thumbnail Guide](thumbnails/overview.md) - Raster and vector thumbnail generation
- [Examples](examples/minimal-workflow.md) - Sample workflows

## Key Features

- **Workflow orchestration**: Load configuration and execute ordered engine sections.
- **Configuration parsing**: Resolve environment values, tags, dataset references, and logging options.
- **Shared data and logging infrastructure**: Use the topic guides for dataset backends and workflow logging.

## Architecture Overview

```
d3tools (orchestration layer)
├── DOOR    - Data downloaders (ERA5, CHIRPS, MODIS, etc.)
├── DAM     - Data processing workflows
└── DRYES   - Drought indices calculation
```

d3tools provides:
- **WorkflowDefinition**: Runnable workflow container
- **Options**: Enhanced configuration mapping
- **WorkflowSection**: Engine-resolved section entries

Detailed behavior is owned by the corresponding topic pages: [configuration](configuration/overview.md),
[datasets](configuration/datasets.md), [timestepping](timestepping.md), and
[logging](logging/overview.md).

## Documentation Structure

### Getting Started
- [Installation and Setup](getting-started.md#installation)
- [Configuration Model](getting-started.md#the-configuration-model)
- [Your First Workflow](getting-started.md#your-first-workflow)
- [Command-Line Execution](getting-started.md#running-from-the-command-line)
- [Tags and Environment Variables](getting-started.md#tags-and-environment-variables)
- [Dataset References](getting-started.md#dataset-references)
- [Logging](getting-started.md#logging)

### Configuration
- [Configuration Overview](configuration/overview.md)
- [Tags and Variables](configuration/tags.md)
- [Dataset Definitions](configuration/datasets.md)
- [Environment Variables](configuration/environment.md)
- [Workflow Sections](configuration/workflow-sections.md)

### Dataset Reference
- [Dataset Concepts](datasets/concepts.md)
- [Dataset Operations](datasets/operations.md)
- [Dataset Time and Availability](datasets/time-and-availability.md)
- [Dataset Composition](datasets/composition.md)
- [Dataset Storage Backends](datasets/storage-backends.md)
- [Dataset Formats and Mixins](datasets/formats-and-mixins.md)
- [Dataset Catalogues and Templates](datasets/catalogues-and-templates.md)
- [Timestepping](timestepping.md)

### Logging
- [Logging Overview](logging/overview.md)
- [Dataset Logging](logging/dataset-logging.md)
- [WorkflowLogManager](logging/workflow-log-manager.md)
- [Logging Examples](logging/examples.md)

### Thumbnails
- [Thumbnail Generation](thumbnails/overview.md)
- Dataset JSON thumbnail definitions
- API usage and available save options

### Examples
- [Minimal Workflow](examples/minimal-workflow.md)
- [Workflow with Logging](examples/with-logging.md)
- [Multi-Dataset Workflow](examples/multi-dataset.md)

## Contributing

d3tools is maintained by CIMA Research Foundation. For questions or contributions, please refer to the repository maintainers.

## Licence

See [LICENSE](../LICENSE) file for details.
