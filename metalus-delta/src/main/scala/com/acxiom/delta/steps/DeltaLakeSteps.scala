package com.acxiom.delta.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

@StepObject
object DeltaLakeSteps {

  @StepFunction("6e6aa49d-e195-4f4f-8933-55045876ef4d",
    "Update Single Column for Deltalake",
    "Updates a single column for a deltalake table.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table")),
    "column" -> StepParameter(None, Some(true), None, None, None, Some("The column to update")),
    "value" -> StepParameter(None, Some(true), None, None, None, Some("The value expression to use")),
    "where" -> StepParameter(None, Some(false), None, None, None, Some("An optional where clause"))))
  def updateSingle(path: String, column: String, value: String, where: Option[String], pipelineContext: PipelineContext): Unit = {
    update(path, Map(column -> value), where, pipelineContext)
  }

  @StepFunction("14744405-f321-4caf-be8d-7e8764385aab",
    "Update Deltalake Table",
    "Updates one or more columns for a deltalake table.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table")),
    "set" -> StepParameter(None, Some(true), None, None, None, Some("Map of column names and update expressions")),
    "condition" -> StepParameter(None, Some(false), None, None, None, Some("An optional where clause"))))
  def update(path: String, set: Map[String, String], condition: Option[String], pipelineContext: PipelineContext): Unit = {
    val table = DeltaTable.forPath(pipelineContext.sparkSession.get, path)
    if (condition.isDefined) {
      table.updateExpr(condition.get, set)
    } else {
      table.updateExpr(set)
    }
  }

  @StepFunction("40856e4e-dcc0-4658-8982-b7a849c38694",
    "Delete Deltalake Table",
    "Delete records from a deltalake table.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table")),
    "condition" -> StepParameter(None, Some(false), None, None, None, Some("the condition used to delete records"))))
  def delete(path: String, condition: String, pipelineContext: PipelineContext): Unit = {
    DeltaTable.forPath(pipelineContext.sparkSession.get, path).delete(condition)
  }

  @StepFunction("170c3d35-1047-42c1-8494-f5c9a667f3e8",
    "Vacuum Deltalake Table",
    "Vacuum records from a deltalake table.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table")),
    "retentionHours" -> StepParameter(Some("double"), Some(false), None, None, None, Some("The hours of data to retain"))))
  def vacuum(path: String, retentionHours: Option[Double] = None, pipelineContext: PipelineContext): Unit = {
    val table = DeltaTable.forPath(pipelineContext.sparkSession.get, path)
    if (retentionHours.isDefined) table.vacuum(retentionHours.get) else table.vacuum()
  }

  @StepFunction("95117379-bbac-400d-b9f2-dcb6ab2f2fc9",
    "Get Delta Table History",
    "Get the history dataFrame for a delta table.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table")),
    "limit" -> StepParameter(Some("int"), Some(false), None, None, None, Some("The number of previous commands to retrieve"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def history(path: String, limit: Option[Int], pipelineContext: PipelineContext): DataFrame = {
    val table = DeltaTable.forPath(pipelineContext.sparkSession.get, path)
    if (limit.isDefined) table.history(limit.get) else table.history()
  }


  @StepFunction("79c130a7-6113-4e49-868d-a518f7eadefc",
    "Upsert Deltalake Table",
    "Merge a dataFrame with a deltalake table, updating matched columns and insert all others.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table.")),
    "source" -> StepParameter(None, Some(true), None, None, None, Some("The source DataFrame to merge into the delta table.")),
    "mergeCondition" -> StepParameter(None, Some(true), None, None, None, Some("The the join condition for the merge.")),
    "sourceAlias" -> StepParameter(None, Some(false), Some("source"), None, None, None, Some("The alias for the source table.")),
    "targetAlias" -> StepParameter(None, Some(false), Some("target"), None, None, None, Some("The alias for the delta table.")),
    "whenMatched" -> StepParameter(None, Some(false), None, None, None, Some("Optional condition for the whenMatched clause.")),
    "whenNotMatched" -> StepParameter(None, Some(false), None, None, None, Some("Optional condition for the whenNotMatched clause."))))
  def upsert(path: String, source: DataFrame, mergeCondition: String,
            sourceAlias: Option[String] = None,
            targetAlias: Option[String] = None,
            whenMatched: Option[String] = None,
            whenNotMatched: Option[String] = None,
            pipelineContext: PipelineContext): Unit = {
    val table = DeltaTable.forPath(pipelineContext.sparkSession.get, path).as(targetAlias.getOrElse("target"))
    val builder = table.merge(source.as(sourceAlias.getOrElse("source")), mergeCondition)
    val update = whenMatched.map(builder.whenMatched).getOrElse(builder.whenMatched()).updateAll()
    whenNotMatched.map(update.whenNotMatched).getOrElse(update.whenNotMatched()).insertAll()
      .execute()
  }

  @StepFunction("5be03a66-094d-4831-9339-b3e8ad89a8b2",
    "Merge Deltalake Table",
    "Merge a dataFrame with a deltalake table.",
    "Pipeline",
    "Deltalake")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, Some("The path to the deltalake table.")),
    "source" -> StepParameter(None, Some(true), None, None, None, Some("The source DataFrame to merge into the delta table.")),
    "mergeCondition" -> StepParameter(None, Some(true), None, None, None, Some("The the join condition for the merge.")),
    "sourceAlias" -> StepParameter(None, Some(false), Some("source"), None, None, None, Some("The alias for the source table. Default is 'source'.")),
    "targetAlias" -> StepParameter(None, Some(false), Some("target"), None, None, None, Some("The alias for the delta table. Default is 'target'.")),
    "whenMatched" -> StepParameter(Some("object"), Some(false), None, Some("com.acxiom.delta.steps.MatchCondition"),
      None, Some("Condition and expression pair for matched records.")),
    "deleteWhenMatched" -> StepParameter(Some("object"), Some(false), None, Some("com.acxiom.delta.steps.MatchCondition"),
      None, Some("Condition for deleting records when matched.")),
    "whenNotMatched" -> StepParameter(Some("object"), Some(false), None, Some("com.acxiom.delta.steps.MatchCondition"),
      None, Some("Condition and expression pair for insert records when not matched."))))
  def merge(path: String, source: DataFrame, mergeCondition: String,
            sourceAlias: Option[String] = None,
            targetAlias: Option[String] = None,
            whenMatched: Option[MatchCondition] = None,
            deleteWhenMatched: Option[MatchCondition] = None,
            whenNotMatched: Option[MatchCondition] = None,
            pipelineContext: PipelineContext): Unit = {
    val table = DeltaTable.forPath(pipelineContext.sparkSession.get, path).as(targetAlias.getOrElse("target"))
    val builder = table.merge(source.as(sourceAlias.getOrElse("source")), mergeCondition)
    val update = whenMatched.map{m =>
      val bldr = m.matchCondition.map(builder.whenMatched).getOrElse(builder.whenMatched())
      m.expressions.map(bldr.updateExpr).getOrElse(bldr.updateAll())
    }.getOrElse(builder)
    val delete = deleteWhenMatched.map(_.matchCondition.map(update.whenMatched).getOrElse(update.whenMatched()).delete()).getOrElse(update)
    whenNotMatched.map{m =>
      val bldr = m.matchCondition.map(delete.whenNotMatched).getOrElse(delete.whenNotMatched())
      m.expressions.map(bldr.insertExpr).getOrElse(bldr.insertAll())
    }.getOrElse(delete).execute()
  }
}

case class MatchCondition(matchCondition: Option[String] = None, expressions: Option[Map[String, String]] = None)
