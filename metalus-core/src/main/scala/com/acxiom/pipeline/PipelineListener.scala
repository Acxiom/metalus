package com.acxiom.pipeline

import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.flow.SplitStepException
import org.apache.log4j.Logger
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.{JsonParser, Serialization}
import org.json4s.{DefaultFormats, Formats}
import java.util.Date

import org.apache.spark.scheduler._

import scala.collection.mutable

object PipelineListener {
  def apply(): PipelineListener = DefaultPipelineListener()
}

case class DefaultPipelineListener() extends SparkListener with PipelineListener {
  private var currentExecutionInfo: Option[PipelineExecutionInfo] = None
  private val applicationStats: ApplicationStats = ApplicationStats(mutable.ListBuffer())

  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    None
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    pipelineContext.setRootAuditMetric("sparkMetrics", "jobs", this.applicationStats.getSummary())
    super.executionFinished(pipelines, pipelineContext)
    applicationStats.reset()
    None
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    pipelineContext.setRootAuditMetric("sparkMetrics", "jobs", this.applicationStats.getSummary())
    super.executionStopped(pipelines, pipelineContext)
    applicationStats.reset()
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
    this.currentExecutionInfo = Some(pipelineContext.getPipelineExecutionInfo)
    None
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
    pipelineContext.setPipelineAuditMetric("sparkMetrics", "jobs", this.applicationStats.getSummary())
    super.pipelineFinished(pipeline, pipelineContext)
    None
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    this.currentExecutionInfo = Some(pipelineContext.getPipelineExecutionInfo)
    super.pipelineStepStarted(pipeline, step, pipelineContext)
    None
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val execInfo = pipelineContext.getPipelineExecutionInfo
    pipelineContext.setStepMetric(
      execInfo.pipelineId.getOrElse(""), execInfo.stepId.getOrElse(""), execInfo.groupId, "jobs", this.applicationStats.getSummary()
    )
    super.pipelineStepFinished(pipeline, step, pipelineContext)
    None
  }

  override def onJobStart(jobStart: SparkListenerJobStart): scala.Unit = {
    if (this.currentExecutionInfo.isDefined) {
      this.applicationStats.startJob(jobStart, this.currentExecutionInfo.get)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): scala.Unit = {
    this.applicationStats.endJob(jobEnd)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.applicationStats.endStage(stageCompleted)
  }
}

trait PipelineListener {
  implicit val formats: Formats = DefaultFormats +
    new EnumNameSerializer(AuditType)
  private val logger = Logger.getLogger(getClass)

  def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    logger.info(s"Starting execution of pipelines ${pipelines.map(p => p.name.getOrElse(p.id.getOrElse("")))}")
    None
  }

  def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    logger.info(s"Finished execution of pipelines ${pipelines.map(p => p.name.getOrElse(p.id.getOrElse("")))}")
    logger.info(s"Execution Audit ${Serialization.write(pipelineContext.rootAudit)}")
    None
  }

  def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    logger.info(s"Stopping execution of pipelines. Completed: ${pipelines.map(p => p.name.getOrElse(p.id.getOrElse(""))).mkString(",")}")
    logger.info(s"Execution Audit ${Serialization.write(pipelineContext.rootAudit)}")
  }

  def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
    logger.info(s"Starting pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
    None
  }

  def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
    logger.info(s"Finished pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
    None
  }

  def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    logger.info(s"Starting step ${step.displayName.getOrElse(step.id.getOrElse(""))} of pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
    None
  }

  def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    logger.info(s"Finished step ${step.displayName.getOrElse(step.id.getOrElse(""))} of pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
    None
  }

  def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    // Base implementation does nothing
  }
}


trait EventBasedPipelineListener extends PipelineListener {
  def key: String
  def credentialName: String
  def credentialProvider: CredentialProvider

  def generateExecutionMessage(event: String, pipelines: List[Pipeline]): String = {
    Serialization.write(Map[String, Any](
      "key" -> key,
      "event" -> event,
      "eventTime" -> new Date().getTime,
      "pipelines" -> pipelines.map(pipeline => EventPipelineRecord(pipeline.id.getOrElse(""), pipeline.name.getOrElse("")))
    ))
  }

  def generateAuditMessage(event: String, audit: ExecutionAudit): String = {
    // Must cast to Long or it won't compile
    val duration = audit.end.getOrElse(Constants.ZERO).asInstanceOf[Long] - audit.start
    val auditString = Serialization.write(audit)
    val auditMap = JsonParser.parse(auditString).extract[Map[String, Any]]
    Serialization.write(Map[String, Any](
      "key" -> key,
      "event" -> event,
      "eventTime" -> new Date().getTime,
      "duration" -> duration,
      "audit" -> auditMap))
  }

  def generatePipelineMessage(event: String, pipeline: Pipeline): String = {
    Serialization.write(Map[String, Any](
      "key" -> key,
      "event" -> event,
      "eventTime" -> new Date().getTime,
      "pipeline" -> EventPipelineRecord(pipeline.id.getOrElse(""), pipeline.name.getOrElse(""))
    ))
  }

  def generatePipelineStepMessage(event: String, pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): String = {
    pipelineContext.getPipelineExecutionInfo.groupId
    Serialization.write(Map[String, Any](
      "key" -> key,
      "event" -> event,
      "eventTime" -> new Date().getTime,
      "pipeline" -> EventPipelineRecord(pipeline.id.getOrElse(""), pipeline.name.getOrElse("")),
      "step" -> EventPipelineStepRecord(step.id.getOrElse(""), step.stepId.getOrElse(""),
        pipelineContext.getPipelineExecutionInfo.groupId.getOrElse(""))
    ))
  }

  def generateExceptionMessage(event: String, exception: PipelineStepException, pipelineContext: PipelineContext): String = {
    val executionInfo = pipelineContext.getPipelineExecutionInfo
    val messageList: MessageLists = exception match {
      case fe: ForkedPipelineStepException => fe.exceptions
        .foldLeft(MessageLists(List[String](), List[Array[StackTraceElement]]()))((t, e) =>
          MessageLists(t.messages :+ e._2.getMessage, t.stacks :+ e._2.getStackTrace))
      case se: SplitStepException => se.exceptions
        .foldLeft(MessageLists(List[String](), List[Array[StackTraceElement]]()))((t, e) =>
          MessageLists(t.messages :+ e._2.getMessage, t.stacks :+ e._2.getStackTrace))
      case p: PauseException => MessageLists(List(s"Paused: ${p.getMessage}"), List())
      case _ => MessageLists(List(exception.getMessage), List(exception.getStackTrace))
    }
    Serialization.write(Map[String, Any](
      "key" -> key,
      "event" -> event,
      "eventTime" -> new Date().getTime,
      "executionId" -> executionInfo.executionId.getOrElse(""),
      "pipelineId" -> executionInfo.pipelineId.getOrElse(""),
      "stepId" -> executionInfo.stepId.getOrElse(""),
      "groupId" -> executionInfo.groupId.getOrElse(""),
      "messages" -> messageList.messages,
      "stacks" -> messageList.stacks
    ))
  }
}


case class SessionVariables(executionComplete: Boolean = false, currentPipeline: Option[Pipeline] = None, currentStep: Option[PipelineStep] = None)
case class EventPipelineRecord(id: String, name: String)
case class EventPipelineStepRecord(id: String, stepId: String, group: String)
case class MessageLists(messages: List[String], stacks: List[Array[StackTraceElement]])
case class CombinedPipelineListener(listeners: List[PipelineListener]) extends PipelineListener {
  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(listeners.foldLeft(pipelineContext)((ctx, listener) => {
      val updatedCtx = listener.executionStarted(pipelines, ctx)
      handleContext(updatedCtx, pipelineContext)
    }))
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(listeners.foldLeft(pipelineContext)((ctx, listener) => {
      val updatedCtx = listener.executionFinished(pipelines, ctx)
      handleContext(updatedCtx, pipelineContext)
    }))
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    listeners.foreach(_.executionStopped(pipelines, pipelineContext))
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
    Some(listeners.foldLeft(pipelineContext)((ctx, listener) => {
      val updatedCtx = listener.pipelineStarted(pipeline, ctx)
      handleContext(updatedCtx, pipelineContext)
    }))
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
    Some(listeners.foldLeft(pipelineContext)((ctx, listener) => {
      val updatedCtx = listener.pipelineFinished(pipeline, ctx)
      handleContext(updatedCtx, pipelineContext)
    }))
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(listeners.foldLeft(pipelineContext)((ctx, listener) => {
      val updatedCtx = listener.pipelineStepStarted(pipeline, step, ctx)
      handleContext(updatedCtx, pipelineContext)
    }))
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(listeners.foldLeft(pipelineContext)((ctx, listener) => {
      val updatedCtx = listener.pipelineStepFinished(pipeline, step, ctx)
      handleContext(updatedCtx, pipelineContext)
    }))
  }

  override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    listeners.foreach(_.registerStepException(exception, pipelineContext))
  }

  private def handleContext(updatedCtx: Option[PipelineContext], pipelineContext: PipelineContext): PipelineContext = {
    if (updatedCtx.isDefined) {
      updatedCtx.get
    } else {
      pipelineContext
    }
  }
}
