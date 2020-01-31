[Documentation Home](readme.md)

# Advanced Step Features
Several advanced step features are made available by Metalus.

## Pipeline Context 
The [PipelineContext](pipeline-context.md) is immutable within a step, however there are a few features in place that allow
steps to update globals and the metrics on the step audit. In order to use these features, a step function must return 
a _PipelineStepResponse_.

### Globals
Using the secondary named map, a step may add a value that begins with _$globals._ to update or add an entry to the globals
object once the step has completed. Any number of these parameters are allowed.

### Metrics
Using the secondary named map, a step may add a value that begins with _$metrics._ to update or add an entry to the step
audit once the step has completed. Any number of these parameters are allowed.
