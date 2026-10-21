# Minimal Workflow Example

The simplest possible d3tools workflow that demonstrates the basic structure without downloads or processing.

## Workflow Configuration

```json
{
    "name": "minimal_workflow",
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 5
    }
}
```

That's it! This is a complete workflow configuration with:
- **name**: Identifies the workflow
- **time**: Defines 5 daily timesteps

## What This Does

When executed, this workflow:
1. Initialises with the name "minimal_workflow"
2. Creates a time range of 5 daily periods
3. Iterates through each period
4. Exits (no sections to execute)

## Running the Workflow

### Save Configuration

Save the JSON as `minimal_workflow.json`.

### Option 1: Using Python

```python
from datetime import datetime
from d3tools import WorkflowDefinition

# Load workflow
workflow = WorkflowDefinition.load('minimal_workflow.json')

# Run for 5 days starting January 1, 2024
workflow.run(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 5)
)

print("Workflow completed!")
```

### Option 2: Using the Command-Line Script

```bash
python scripts/run_workflow.py minimal_workflow.json -s 2024-01-01 -e 2024-01-05
```

## Output

Since there are no workflow sections, there's minimal output:
```
(No output - workflow completes silently)
```

## Adding Workflow Sections

Let's add a simple section:

```json
{
    "name": "minimal_with_section",
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 5
    },
    
    "workflow_sections": []
}
```

Still no sections to execute, but the structure is explicit.

## With Tags

Add configuration variables:

```json
{
    "name": "minimal_with_tags",
    
    "tags": {
        "project": "test_project",
        "region": "test_region",
        "output_dir": "/tmp/output/{project}/{region}"
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 5
    }
}
```

Access tags programmatically:

```python
workflow = WorkflowDefinition.load('minimal_with_tags.json')
print(f"Project: {workflow.project}")  # Outputs: test_project
print(f"Region: {workflow.region}")    # Outputs: test_region
print(f"Output: {workflow.output_dir}") # Outputs: /tmp/output/test_project/test_region
```

## With Datasets

Define a dataset without using it:

```json
{
    "name": "minimal_with_dataset",
    
    "datasets": {
        "test_data": {
            "type": "local",
            "path": "/tmp/data/",
            "filename": "test_%Y%m%d.txt"
        }
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 5
    }
}
```

Access datasets programmatically:

```python
workflow = WorkflowDefinition.load('minimal_with_dataset.json')

# Access the dataset
dataset = workflow.datasets['test_data']
print(f"Dataset type: {dataset.type}")
print(f"Dataset path: {dataset.path}")
```

## Complete Minimal Example

All basic features combined:

```json
{
    "name": "complete_minimal",
    
    "tags": {
        "project": "minimal_example",
        "data_root": "/tmp/data"
    },
    
    "datasets": {
        "example_data": {
            "type": "local",
            "path": "{data_root}/{project}/",
            "filename": "data_%Y%m%d.txt"
        }
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 5
    },
    
    "workflow_sections": []
}
```

## Next Steps

- Add logging: [Workflow with Logging](with-logging.md)
- Add downloads: [Full Pipeline Example](full-pipeline.md)
- Add processing: [Processing Workflow](../configuration/workflow-sections.md)

## Key Takeaways

1. **Minimal workflow needs**: `name` and `time`
2. **Tags** make configuration reusable
3. **Datasets** define data sources
4. **workflow_sections** define what to execute (can be empty)
5. **Workflow runs** even with no sections

## Common Use Cases

### Testing Configuration

Use minimal workflows to test configuration parsing without execution:

```python
# Test if configuration is valid
try:
    workflow = WorkflowDefinition.load('test_config.json')
    print("Configuration is valid!")
except Exception as e:
    print(f"Configuration error: {e}")
```

### Template for New Workflows

Start with minimal template and add sections:

```json
{
    "name": "new_workflow",
    "time": { "type": "fixed_len", "length": "1D", "num_steps": 1 },
    "tags": {},
    "datasets": {},
    "workflow_sections": []
}
```

Then gradually add:
1. Tags for configuration
2. Datasets for data sources
3. Workflow sections for execution

## See Also

- [Getting Started](../getting-started.md)
- [With Logging Example](with-logging.md)
- [Multi-Dataset Example](multi-dataset.md)
- [Configuration Overview](../configuration/overview.md)
