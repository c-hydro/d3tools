# User Guide

## Start here

This guide is for people who write workflow JSON and run it. Start with
[installation and a minimal run](getting-started.md), then use the task guides
for [configuration](configuration.md), [datasets](datasets.md),
[workflows and execution ranges](workflows.md), and [logging](logging.md).
The [CHIRPS-to-SPI case study](case-study.md) is the first complete
engine-backed journey; [examples](examples.md) and
[troubleshooting](troubleshooting.md) help with common next steps.

d3tools reads workflow JSON, resolves its shared configuration, and runs
sections in order. DOOR, DAM, and DRYES own the payload schemas and domain
behavior inside their sections. For implementation, extension, or Python use
beyond `WorkflowDefinition.load()` and `run()`, see the
[Developer Guide](../developer/index.md).
