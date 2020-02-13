[Documentation Home](readme.md)

# Pipeline Manager
The pipeline manager is used to retrieve pipelines within an [application](applications.md) or [step group](step-groups.md).

## Cached Pipeline Manager
The default implementation of _PipelineManager_ takes a list of pipelines and serves them by id. Additionally, this 
implementation will attempt to scan the step libraries **metadata/pipelines** path looking for the pipeline. The pipeline 
must be in a JSON file with the following naming convention: ```<pipeline.id>.json```

## Custom Implementation
Developers may create a custom implementation by implementing the _PipelineManager_ trait and overriding the _getPipeline_
function. Implementation functions that are not able to resolve pipelines should call _super.getPipeline_ as a default 
to have the classpath will be scanned.

### Example File Implementation
This example illustrates how to implement a new _PipelineManager_ that loads pipelines from disk. Walking through the 
implementation, the new class _LocalFilePipelineManager_ extends _PipelineManager_ with a single constructor parameter
named _path_ which indicates where to look for [pipeline json](json-pipelines.md). **NOTE:** Implementations could load 
pipeline classes instead of JSON based pipelines.

Next the _getPipeline_ function is overridden to look for pipelines in the path provided in the constructor.

```scala
class LocalFilePipelineManager(path: String) extends PipelineManager {
  override def getPipeline(id: String): Option[Pipeline] = {
    if (new java.io.File(path, s"$id.json").exists()) {
      val pipelineList = DriverUtils.parsePipelineJson(Source.fromFile(new java.io.File(path, s"$id.json")).mkString)
      if (pipelineList.isDefined && pipelineList.get.nonEmpty) {
        Some(pipelineList.get.head)
      } else {
        None
      }
    } else {
      super.getPipeline(id)
    }
  }
}
```
