package com.acxiom.pipeline.streaming

import com.acxiom.pipeline.utils.StreamingUtils
import com.acxiom.pipeline.{Constants, PipelineContext, PipelineException}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQuery

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

trait StreamingQueryMonitor extends Thread {
  protected val logger: Logger = Logger.getLogger(getClass)

  def query: StreamingQuery
  def pipelineContext: PipelineContext
  def getGlobalUpdates: Map[String, Any] = Map()
  def continue: Boolean = false
}

class BaseStreamingQueryMonitor(override val query: StreamingQuery, override val pipelineContext: PipelineContext)
  extends StreamingQueryMonitor

abstract class BatchWriteStreamingQueryMonitor(override val query: StreamingQuery, override val pipelineContext: PipelineContext)
  extends BaseStreamingQueryMonitor(query, pipelineContext) {
  protected val monitorType: String = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_MONITOR_TYPE").getOrElse("duration").toLowerCase
  protected val duration: Int = pipelineContext.getGlobalAs[Int]("STREAMING_BATCH_MONITOR_DURATION").getOrElse(Constants.ONE_THOUSAND * Constants.SIXTY)
  protected val approximateRows: Int = pipelineContext.getGlobalAs[Int]("STREAMING_BATCH_MONITOR_COUNT").getOrElse(Constants.ZERO)
  protected val sleepDuration: Long =
    if (monitorType == "duration") {
      // Set the sleep for the duration specified
      val durationType = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_MONITOR_DURATION_TYPE").getOrElse("milliseconds")
      StreamingUtils.getDuration(Some(durationType), Some(duration.toString)).milliseconds
    } else {
      0L
    }

  protected var startTime: Long = System.currentTimeMillis()
  protected var rowCount: Long = 0L
  protected var currentDuration: Long = 0L
  protected var lastStatusId: UUID = _
  protected var globals: Map[String, Any] = Map()
  protected var continueProcessing = false

  override def getGlobalUpdates: Map[String, Any] = globals
  override def continue: Boolean = continueProcessing

  protected var processing = true
  def keepProcessing: Boolean = processing
  def checkCurrentStatus(): Unit = {
    // See if we have reached the specified duration
    processing = if ((monitorType == "duration" && currentDuration >= sleepDuration) ||
      (rowCount >= approximateRows)) {
      logger.info("Streaming threshold met")
      startTime = System.currentTimeMillis()
      currentDuration = 0L
      rowCount = 0
      false
    } else {
      true
    }
  }

  def manageQueryShutdown(): Unit

  override def run(): Unit = {
    logger.info("Starting streaming batch monitor")
    while (keepProcessing) {
      // Do the sleep at the beginning assuming that we want to process some data before we check the status
      Thread.sleep(Constants.ONE_THOUSAND)
      // Capture the current run length
      currentDuration = System.currentTimeMillis() - startTime
      // Update the stats - The array should be oldest to most recent
      val index = query.recentProgress.indexWhere(_.id == lastStatusId)
      val progressList = if (index != -1) {
        query.recentProgress.toList.slice(index, query.recentProgress.toList.size)
      } else {
        query.recentProgress.toList
      }
      progressList.foreach(p => {
        rowCount += p.numInputRows
        lastStatusId = p.id
      })
      // Call the functions to determine if we need to stop or keep going
      checkCurrentStatus()
    }
    // Invoke the function that allows us to create the globals and set the continue flag
    manageQueryShutdown()
    // Stop the query once we are no longer processing
    logger.info("Streaming query being stopped")
    query.stop()
  }
}

class BatchPartitionedStreamingQueryMonitor(override val query: StreamingQuery, override val pipelineContext: PipelineContext)
  extends BatchWriteStreamingQueryMonitor(query, pipelineContext) {
  logger.info("Created BatchPartitionedStreamingQueryMonitor")
  private val dateFormat =new SimpleDateFormat("yyyy-dd-MM HH:mm:ssZ")

  override def manageQueryShutdown(): Unit = {
    val counter = pipelineContext.getGlobalAs[Int]("STREAMING_BATCH_PARTITION_COUNTER").getOrElse(0) + 1
    val globalKey = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_PARTITION_GLOBAL").getOrElse("PARTITION_VALUE")
    val template = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_PARTITION_TEMPLATE").getOrElse("counter").toLowerCase
    val temp = if (template == "date") {
      dateFormat.format(new Date())
    } else {
      counter.toString
    }
    logger.info(s"Setting $globalKey to $temp")
    logger.info(s"Setting STREAMING_BATCH_OUTPUT_COUNTER to $counter")
    globals = Map[String, Any]("STREAMING_BATCH_OUTPUT_COUNTER" -> counter, globalKey -> temp)
    continueProcessing = true
  }
}

class BatchFileStreamingQueryMonitor(override val query: StreamingQuery, override val pipelineContext: PipelineContext)
  extends BatchWriteStreamingQueryMonitor(query, pipelineContext) {
  logger.info("Created BatchFileStreamingQueryMonitor")

  override def manageQueryShutdown(): Unit = {
    validate()
    val globalDestinationKey = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_OUTPUT_GLOBAL").get
    val destinationKey = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_OUTPUT_PATH_KEY").get
    val template = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_OUTPUT_TEMPLATE").getOrElse("DATE").toLowerCase
    val counter = pipelineContext.getGlobalAs[Int]("STREAMING_BATCH_OUTPUT_COUNTER").getOrElse(0) + 1
    val temp = if (template == "date") {
      Constants.FILE_APPEND_DATE_FORMAT.format(new Date())
    } else {
      counter.toString
    }

    // Grab the path to be modified
    val globalDestination = pipelineContext.getGlobalAs[String](globalDestinationKey).get
    // See if we have already stored the original path
    val originalPath = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_OUTPUT_GLOBAL_ORIGINAL_PATH")
    if (originalPath.isEmpty) {
      // Set the original path for use later and use the existing destination
      globals = Map[String, Any]("STREAMING_BATCH_OUTPUT_COUNTER" -> counter,
      "STREAMING_BATCH_OUTPUT_GLOBAL_ORIGINAL_PATH" -> globalDestination,
        globalDestinationKey -> updatePath(globalDestination, destinationKey, temp, globalDestinationKey))
    } else {
      // Use the stored original path so we don't stack the increment values
      globals = Map[String, Any]("STREAMING_BATCH_OUTPUT_COUNTER" -> counter,
        globalDestinationKey -> updatePath(originalPath.get, destinationKey, temp, globalDestinationKey))
    }
    logger.info(s"Setting STREAMING_BATCH_OUTPUT_COUNTER to $counter")
    continueProcessing = true
  }

  private def updatePath(path: String, destinationKey: String, incrementVal: String, globalDestinationKey: String): String = {
    // Update the original path with the type parameter
    val newPath = path.replaceAll(destinationKey, s"${destinationKey}_$incrementVal")
    logger.info(s"Setting $globalDestinationKey to $newPath")
    newPath
  }

  private def validate(): Unit = {
    val globals = pipelineContext.globals.get
    if (!globals.contains("STREAMING_BATCH_OUTPUT_GLOBAL")) {
      logger.error("The STREAMING_BATCH_OUTPUT_GLOBAL value must be set!")
      throw PipelineException(message = Some("The STREAMING_BATCH_OUTPUT_GLOBAL value must be set!"),
        pipelineProgress = Some(pipelineContext.getPipelineExecutionInfo))
    }
    if (!globals.contains("STREAMING_BATCH_OUTPUT_PATH_KEY")) {
      logger.error("The STREAMING_BATCH_OUTPUT_PATH_KEY value must be set!")
      throw PipelineException(message = Some("The STREAMING_BATCH_OUTPUT_PATH_KEY value must be set!"),
        pipelineProgress = Some(pipelineContext.getPipelineExecutionInfo))
    }
    val destinationKey = pipelineContext.getGlobalAs[String]("STREAMING_BATCH_OUTPUT_PATH_KEY")
    if (destinationKey.isEmpty) {
      logger.error(s"The $destinationKey is required!")
      throw PipelineException(message = Some(s"The $destinationKey is required!"),
        pipelineProgress = Some(pipelineContext.getPipelineExecutionInfo))
    }
  }
}

