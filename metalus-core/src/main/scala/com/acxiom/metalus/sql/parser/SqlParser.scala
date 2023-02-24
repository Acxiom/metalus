package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.{EngineMeta, Parameter, PipelineStep}
import com.acxiom.metalus.sql.parser.MSqlParser._
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream, RuleContext}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object SqlParser {

  def parse(sql: String): List[PipelineStep] = {
    val cs = UpperCaseCharStream.fromString(sql)
    val lexer = new MSqlLexer(cs)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val cts = new CommonTokenStream(lexer)
    val parser = new MSqlParser(cts)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    new SqlParser(cts).visit(parser.singleStatement())
  }

}

class SqlParser(tokenStream: CommonTokenStream) extends MSqlParserBaseVisitor[List[PipelineStep]] {

  private val stepIds: ListBuffer[String] = ListBuffer()

  // handle select, where, groupBy, and having
  override def visitQuerySpecification(ctx: QuerySpecificationContext): List[PipelineStep] = {
    val fromJoinSteps = visit(ctx.relation())
    val where = Option(ctx.where).map{ where =>
      buildAndChainStep("where", fromJoinSteps, parameters = Some(List(
        buildStepParameter("expression", getText(where.booleanExpression()), Some("expression"))
      )))
    }.getOrElse(fromJoinSteps)
    val groupBy = Option(ctx.groupBy()).map { groupBy =>
      buildAndChainStep("groupBy", where, parameters = Some(List(
        buildStepParameter("expressions", groupBy.groupingElement().asScala.map(getText), Some("expression"))
      )))
    }.getOrElse(where)
    val having = Option(ctx.having).map { having =>
      buildAndChainStep("having", groupBy, parameters = Some(List(
        buildStepParameter("expression", getText(having), Some("expression"))
      )))
    }.getOrElse(groupBy)
    buildAndChainStep("select", having, parameters = Some(List(
      buildStepParameter("expressions", ctx.selectItem().asScala.map(getText), Some("expression"))
    )))
  }

  override def visitQueryNoWith(ctx: QueryNoWithContext): List[PipelineStep] = {
    val select = visit(ctx.queryTerm())
    val orderBy = Option(ctx.sortItem()).map(_.asScala).filter(_.nonEmpty).map{ orderBy =>
      buildAndChainStep("orderBy", select, parameters = Some(List(
        buildStepParameter("expressions", orderBy.map(getText), Some("expression"))
      )))
    }.getOrElse(select)
    Option(ctx.limit).orElse(Option(ctx.fetchFirstNRows)).map{ limit =>
      buildAndChainStep("limit", orderBy, parameters = Some(List(
        buildStepParameter("limit", limit.getText, Some("integer"))
      )))
    }.getOrElse(orderBy)
  }

  override def visitUpdate(ctx: UpdateContext): List[PipelineStep] = {
    val update = buildAndChainStep("update", visit(ctx.dataReference()), parameters = Some(List(
      buildStepParameter("expressions", ctx.setExpression().asScala.map(getText), Some("expression"))
    )))
    Option(ctx.where).map { where =>
      buildAndChainStep("where", update, parameters = Some(List(
        buildStepParameter("expression", getText(where.booleanExpression()), Some("expression"))
      )))
    }.getOrElse(update)
  }

  override def visitDelete(ctx: DeleteContext): List[PipelineStep] = {
    val delete = buildAndChainStep("delete", visit(ctx.dataReference()))
    Option(ctx.where).map { where =>
      buildAndChainStep("where", delete, parameters = Some(List(
        buildStepParameter("expression", getText(where.booleanExpression()), Some("expression"))
      )))
    }.getOrElse(delete)
  }

  override def visitTruncateTable(ctx: TruncateTableContext): List[PipelineStep] = {
    buildAndChainStep("truncate", visit(ctx.dataReference()))
  }

  override def visitDropTable(ctx: DropTableContext): List[PipelineStep] = {
    val params = List(buildStepParameter("view", ctx.dropType.getType == MSqlParser.VIEW, Some("boolean")),
      buildStepParameter("ifExists", Option(ctx.EXISTS()).exists(_.getText.nonEmpty), Some("boolean")))
    buildAndChainStep("drop", visit(ctx.dataReference()), parameters = Some(params))
  }

  // handle table/query aliases
  override def visitAliasedRelation(ctx: AliasedRelationContext): List[PipelineStep] = {
    val steps = visit(ctx.relationPrimary())
    Option(ctx.identifier()).map{ alias =>
      buildAndChainStep("as", steps, parameters = Some(List(
        buildStepParameter("alias", alias.getText, Some("string")))),
        idSuffix = Some(alias.getText)
      )
    }.getOrElse(steps)
  }

  override def visitDataReference(ctx: DataReferenceContext): List[PipelineStep] = {
    Option(ctx.step()).map(visitStep).getOrElse{
      val id = buildStepId("DR", ctx.mapping().key.getText.replaceAllLiterally(".", "_"))
      stepIds += id
      List(
        PipelineStep(
          Some(id),
          `type` = Some("pipeline"),
          value = Some(ctx.mapping().getText)
        )
      )
    }
  }

  override def visitStep(ctx: StepContext): List[PipelineStep] = {
    val id = buildStepId("STEP", ctx.stepName.getText)
    stepIds += id
    val params = ctx.stepParam().asScala.map{
      case paramCtx if paramCtx.stepParamName.isEmpty =>
        buildStepParameter(paramCtx.stepValue().getText, paramCtx.stepValue().getText)
      case paramCtx =>
        buildStepParameter(paramCtx.stepParamName.getText, paramCtx.stepValue().getText)
    }.toList
    List(
      PipelineStep(
        Some(id),
        `type` = Some("pipeline"),
        params = if (params.isEmpty) None else Some(params),
        value = Some("STEP"),
        engineMeta = Some(EngineMeta(Some("QueryingSteps." + ctx.stepName.getText)))
      )
    )
  }

  override def visitJoinRelation(ctx: JoinRelationContext): List[PipelineStep] = {
    val left = visit(ctx.left)
    val right = visit(Option(ctx.right).getOrElse(ctx.rightRelation))

    val joinTypeParam = Option(ctx.CROSS()).map(_.getText)
      .orElse(Option(ctx.NATURAL()).map( _.getText + " " + Option(ctx.joinType()).mkString))
      .orElse(Option(ctx.joinType()).map(getText))
      .filter(_.nonEmpty)
      .map(jt => buildStepParameter("joinType", jt, Some("string")))

    val expressionParam = Option(ctx.joinCriteria()).flatMap { jc =>
      Option(jc.booleanExpression()).map { b =>
        buildStepParameter("condition", getText(b), Some("expression"))
      } orElse Option(jc.identifier()).map { i =>
        buildStepParameter("using", i.asScala.map(getText), Some("expression"))
      }
    }
    val params = joinTypeParam ++ expressionParam
    buildAndChainStep("join", left, Some(right), Some(params.toList))
  }

  override def defaultResult(): List[PipelineStep] = List()

  override def aggregateResult(aggregate: List[PipelineStep], nextResult: List[PipelineStep]): List[PipelineStep] = {
    nextResult.headOption.map{ next =>
      val modStep = aggregate.lastOption.map(_.copy(nextSteps = Some(List("'" + next.id.mkString + "'"))))
      aggregate.dropRight(1) ++ modStep ++ nextResult

    }.getOrElse(aggregate)
  }

  private def buildAndChainStep(operation: String, left: List[PipelineStep],
                                right: Option[List[PipelineStep]] = None,
                                parameters: Option[List[Parameter]] = None,
                                value: Option[String] = None,
                                idSuffix: Option[String] = None): List[PipelineStep] = {
    val leftId = left.last.id.mkString
    val rightId = right.map(_.last.id.mkString)
    val id = idSuffix.map(id => buildStepId(operation.toUpperCase, id))
      .getOrElse(buildStepId(operation.toUpperCase, leftId, rightId))
    stepIds += id
    val params = (buildStepParameter(if (right.isDefined) "left" else "dataReference", "@" + leftId) +:
      parameters.getOrElse(List())) ++ rightId.map(id => buildStepParameter("right", "@" + id))
    val step = PipelineStep(
      Some(id),
      `type` = Some("pipeline"),
      params = Some(params),
      value = value.orElse(Some("STEP")),
      engineMeta = Some(EngineMeta(Some("QueryingSteps." + operation)))
    )
    aggregateResult(right.map(aggregateResult(left, _)).getOrElse(left), List(step))
  }

  private def buildStepParameter(name: String, value: Any, paramType: Option[String] = None): Parameter = {
    Parameter(paramType.orElse(Some("text")),
      Some(name.replaceAll("[^a-zA-Z0-9]", "")),
      required = Some(true),
      value = Some(value)
    )
  }

  private def buildStepId(operation: String, left: String, right: Option[String] = None): String = {
    val id = operation + "_" + left.substring(left.indexOf("_") + 1) +
      right.map(r => "_" + r.substring(r.indexOf("_") + 1)).mkString
    val duplicateCount = stepIds.count ( oldId => oldId == id || oldId.matches(id + "_[\\d]+"))
    if (duplicateCount > 0) id + "_" + duplicateCount else id
  }

  private def getText(ctx: RuleContext): String = tokenStream.getText(ctx)

}
