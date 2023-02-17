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
    val params = (buildStepParameter(if(right.isDefined) "left" else "dataReference", "@" + leftId) +:
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

//  private def buildAndChainStep(operation: String,
//                                steps: List[PipelineStep],
//                                extraParameters: Option[List[Parameter]] = None,
//                                value: Option[String] = None,
//                                idSuffix: Option[String] = None,
//                                drParamName: Option[String] = None): List[PipelineStep] = {
//    val lastId = steps.last.id.mkString
//    val id = buildStepId(operation.toUpperCase, idSuffix.getOrElse(lastId))
//    stepIds += id
//    val params = buildStepParameter(drParamName.getOrElse("dataReference"), "@" + lastId) +: extraParameters.getOrElse(List())
//    val step = PipelineStep(
//      Some(id),
//      `type` = Some("pipeline"),
//      params = Some(params),
//      value = value.orElse(Some("STEP")),
//      engineMeta = Some(EngineMeta(Some("QueryingSteps." + operation)))
//    )
//    aggregateResult(steps, List(step))
//  }

  // handle select, where, groupBy, and having
  override def visitQuerySpecification(ctx: QuerySpecificationContext): List[PipelineStep] = {
    val fromJoinSteps = visit(ctx.relation())
    val where = Option(ctx.where).map{ where =>
      buildAndChainStep("where", fromJoinSteps, parameters = Some(List(
        buildStepParameter("expression", getText(where), Some("expression"))
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

//    val rightParam = buildStepParameter("left", "@" + left.last.id.mkString)
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
      aggregate.dropRight(1) ++ modStep  ++ nextResult

    }.getOrElse(aggregate)
  }

  private def buildStepParameter(name: String, value: Any, stepType: Option[String] = None): Parameter = {
    Parameter(stepType.orElse(Some("text")),
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
