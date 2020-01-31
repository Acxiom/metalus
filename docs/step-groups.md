[Documentation Home](readme.md)

# Step Group
The step-group type step provides a mechanism for embedding pipelines within another pipeline. This feature is in place
to leverage building smaller reusable pipelines that can be shared across other pipelines. The pipeline will be provided 
with a pipeline context with no pipeline parameters and globals that are only populated with the values from the 
pipelineMappings parameter. There are three parameters for a step-group:

#### pipelineId
This parameter must be a pipeline id that is accessible to the *PipelineManager* within the *PipelineContext*. This
parameter is not required if the **pipeline** parameter is used.

#### pipeline
This parameter is required and can contain either a string with expansion variable or a map with the proper pipeline 
layout. The "className" attribute must be set when using a map. This parameter is not required if the **pipelineId**
parameter is used.

#### pipelineMappings
This optional parameter provides a mechanism for mapping values from the outer pipeline to the globals object accessible 
to the embedded pipeline.
