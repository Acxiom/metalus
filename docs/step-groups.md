[Documentation Home](readme.md)

# Step Group
A step-group provides a mechanism for embedding pipelines within another pipeline. This feature is in place
to leverage building smaller reusable pipelines that can be shared across other pipelines. The pipeline will be provided 
with a pipeline context with no pipeline parameters and globals that are only populated with the values from the 
pipelineMappings parameter.

## Step Group Result
The _stepGroupResult_ parameter of a pipeline defines a mapping that provides a single primary response that can be 
referenced easily from other steps. Normal mappings apply as well as [scalascript](./parameter-mapping.md#types) 
mappings. This will override the [default return](#default-return) by placing the mapped value into the primary response 
and the [default return](#default-return) into the _namedReturns_ response.

## Parameters
There are five parameters for a step-group:

### pipelineId
This parameter must be a pipeline id that is accessible to the [PipelineManager](pipeline-manager.md) within the 
[PipelineContext](pipeline-context.md). This parameter can be made optional by providing the **pipeline** parameter.

### pipeline
This required parameter can contain either a string with expansion variable or a map with the proper pipeline 
layout. The "className" attribute must be set when using a map. This parameter can be made optional by providing the 
**pipelineId** parameter.

### pipelineMappings
This optional parameter provides a mechanism for mapping values from the outer pipeline to the globals object accessible 
to the embedded pipeline.

### useParentGlobals
This boolean parameter indicates whether the step group should have access to the parent globals. The default value of 
false will seed the globals from the _pipelineMappings_ only. When true, the parent globals will be used as a base with the
_pipelineMappings_ applied as an addition and override.

### output
This optional _result_ parameter works the same as the [Step Group Result](#step-group-result).

## Default Return
A _PipelineStepResponse_ containing a map as the primary return type and nothing for the secondary return type. The map
keys will be the pipeline step ids of each step that was executed for the _step-group_ pipeline.

### Global Updates
Steps within a step-group may update the root _globals_.

### Execution Audits
Like other pipelines, audits will be captured during execution. These audits will be attached to the pipeline step. 
