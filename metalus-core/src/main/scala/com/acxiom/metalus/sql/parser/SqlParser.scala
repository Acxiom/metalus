package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.sql.{DataReference, Expression, Join, QueryOperator}
import com.acxiom.metalus.{EngineMeta, Parameter, PipelineContext, PipelineException, PipelineStep}
import com.acxiom.metalus.sql.parser.MSqlParser._
import com.acxiom.metalus.steps.QueryingSteps.applyQueryOperation
import com.acxiom.metalus.utils.ReflectionUtils
import org.antlr.v4.runtime.{CommonTokenStream, RuleContext, TokenStream}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object SqlParser {

  def parseToSteps(sql: String): List[PipelineStep] = {
    val parser = getParser(sql)
    new SqlPipelineBuilder(parser.getTokenStream).visit(parser.singleStatement())
  }

  def parseToDataReference(sql: String, pipelineContext: PipelineContext): DataReference[_] = {
    val parser = getParser(sql)
    new DataReferenceBuilder(parser.getTokenStream, pipelineContext).visit(parser.singleStatement())
  }

  private def getParser(sql: String): MSqlParser = {
    val cs = UpperCaseCharStream.fromString(sql)
    val lexer = new MSqlLexer(cs)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val cts = new CommonTokenStream(lexer)
    val parser = new MSqlParser(cts)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser
  }

}

trait SqlParser[T, P] extends MSqlParserBaseVisitor[T] {

  private lazy val cts = tokenStream

  protected def tokenStream: TokenStream

  protected def buildAndChain(operation: String, left: T,
                              right: Option[T] = None,
                              parameters: Option[List[P]] = None): T

  protected def buildParameter(name: String, ctx: RuleContext): P

  protected def buildParameter(name: String, ctx: List[RuleContext]): P

  protected def buildParameter(name: String, text: String, typeHint: Option[String] = None): P

  protected def getText(ctx: RuleContext): String = cts.getText(ctx)

  override def visitQuerySpecification(ctx: QuerySpecificationContext): T = {
    val fromJoinSteps = visit(ctx.relation())
    val where = Option(ctx.where).map { where =>
      buildAndChain("where", fromJoinSteps, parameters = Some(List(
        buildParameter("expression", where.booleanExpression())
      )))
    }.getOrElse(fromJoinSteps)
    val groupBy = Option(ctx.groupBy()).map { groupBy =>
      buildAndChain("groupBy", where, parameters = Some(List(
        buildParameter("expressions", groupBy.groupingElement().asScala.toList)
      )))
    }.getOrElse(where)
    val having = Option(ctx.having).map { having =>
      buildAndChain("having", groupBy, parameters = Some(List(
        buildParameter("expression", having)
      )))
    }.getOrElse(groupBy)
    buildAndChain("select", having, parameters = Some(List(
      buildParameter("expressions", ctx.selectItem().asScala.toList)
    )))
  }

  override def visitQueryNoWith(ctx: QueryNoWithContext): T = {
    val select = visit(ctx.queryTerm())
    val orderBy = Option(ctx.sortItem()).map(_.asScala).filter(_.nonEmpty).map { orderBy =>
      buildAndChain("orderBy", select, parameters = Some(List(
        buildParameter("expressions", orderBy.toList)
      )))
    }.getOrElse(select)
    Option(ctx.limit).orElse(Option(ctx.fetchFirstNRows)).map { limit =>
      buildAndChain("limit", orderBy, parameters = Some(List(
        buildParameter("limit", limit.getText, Some("integer"))
      )))
    }.getOrElse(orderBy)
  }

  override def visitUpdate(ctx: UpdateContext): T = {
    val update = buildAndChain("update", visit(ctx.dataReference()), parameters = Some(List(
      buildParameter("expressions", ctx.setExpression().asScala.toList)
    )))
    Option(ctx.where).map { where =>
      buildAndChain("where", update, parameters = Some(List(
        buildParameter("expression", where.booleanExpression())
      )))
    }.getOrElse(update)
  }

  override def visitDelete(ctx: DeleteContext): T = {
    val delete = buildAndChain("delete", visit(ctx.dataReference()))
    Option(ctx.where).map { where =>
      buildAndChain("where", delete, parameters = Some(List(
        buildParameter("expression", where.booleanExpression())
      )))
    }.getOrElse(delete)
  }

  override def visitTruncateTable(ctx: TruncateTableContext): T =
    buildAndChain("truncate", visit(ctx.dataReference()))

  override def visitDropTable(ctx: DropTableContext): T = {
    val params = List(buildParameter("view", (ctx.dropType.getType == MSqlParser.VIEW).toString, Some("boolean")),
      buildParameter("ifExists", Option(ctx.EXISTS()).exists(_.getText.nonEmpty).toString, Some("boolean")))
    buildAndChain("drop", visit(ctx.dataReference()), parameters = Some(params))
  }

  // handle table/query aliases
  override def visitAliasedRelation(ctx: AliasedRelationContext): T = {
    val steps = visit(ctx.relationPrimary())
    Option(ctx.identifier()).map { alias =>
      buildAndChain("as", steps, parameters = Some(List(
        buildParameter("alias", alias.getText)))
      )
    }.getOrElse(steps)
  }

  override def visitJoinRelation(ctx: JoinRelationContext): T = {
    val left = visit(ctx.left)
    val right = visit(Option(ctx.right).getOrElse(ctx.rightRelation))

    val joinTypeParam = Option(ctx.CROSS()).map(_.getText)
      .orElse(Option(ctx.NATURAL()).map(_.getText + " " + Option(ctx.joinType()).mkString))
      .orElse(Option(ctx.joinType()).map(getText))
      .filter(_.nonEmpty)
      .map(jt => buildParameter("joinType", jt, Some("string")))

    val expressionParam = Option(ctx.joinCriteria()).flatMap { jc =>
      Option(jc.booleanExpression()).map { b =>
        buildParameter("condition", b)
      } orElse Option(jc.identifier()).map { i =>
        buildParameter("using", i.asScala.toList)
      }
    }
    val params = joinTypeParam ++ expressionParam
    buildAndChain("join", left, Some(right), Some(params.toList))
  }

}

class SqlPipelineBuilder(override val tokenStream: TokenStream) extends SqlParser[List[PipelineStep], Parameter] {

  private val stepIds: ListBuffer[String] = ListBuffer()

  override def visitDataReference(ctx: DataReferenceContext): List[PipelineStep] = {
    Option(ctx.step()).map(visitStep).getOrElse {
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
    val (pkg, obj, method) = ctx.IDENTIFIER().asScala.map(_.getText).toList match {
      case List(m) => (None, "QueryingSteps.", m)
      case List(obj, m) => (None, obj, m)
      case l =>
        val (pkgList, stepList) = l.splitAt(l.size - 2)
        (Some(pkgList.mkString(".")), stepList.head, stepList.last)
    }
    val id = buildStepId("STEP", method)
    stepIds += id
    val params = ctx.stepParam().asScala.map {
      case paramCtx if paramCtx.stepParamName.isEmpty =>
        buildParameter(paramCtx.stepValue().getText, paramCtx.stepValue().getText)
      case paramCtx =>
        buildParameter(paramCtx.stepParamName.getText, paramCtx.stepValue().getText)
    }.toList
    List(
      PipelineStep(
        Some(id),
        `type` = Some("pipeline"),
        params = if (params.isEmpty) None else Some(params),
        value = Some("STEP"),
        engineMeta = Some(EngineMeta(Some(obj + "." + method), pkg))
      )
    )
  }


  override def defaultResult(): List[PipelineStep] = List()

  override def aggregateResult(aggregate: List[PipelineStep], nextResult: List[PipelineStep]): List[PipelineStep] = {
    nextResult.headOption.map { next =>
      val modStep = aggregate.lastOption.map(_.copy(nextSteps = Some(List(next.id.mkString))))
      aggregate.dropRight(1) ++ modStep ++ nextResult

    }.getOrElse(aggregate)
  }

  private def buildStepId(operation: String, left: String, right: Option[String] = None): String = {
    val id = operation + "_" + left.substring(left.indexOf("_") + 1) +
      right.map(r => "_" + r.substring(r.indexOf("_") + 1)).mkString
    val duplicateCount = stepIds.count(oldId => oldId == id || oldId.matches(id + "_[\\d]+"))
    if (duplicateCount > 0) id + "_" + duplicateCount else id
  }

  override protected def buildAndChain(operation: String, left: List[PipelineStep],
                                       right: Option[List[PipelineStep]],
                                       parameters: Option[List[Parameter]]): List[PipelineStep] = {
    val leftId = left.last.id.mkString
    val rightId = right.map(_.last.id.mkString)
    val id = if (operation equalsIgnoreCase "as") {
      val suffix = parameters.flatMap(_.collectFirst { case p if p.name.contains("alias") => p.value.mkString })
        .getOrElse(leftId)
      buildStepId(operation.toUpperCase, suffix)
    } else {
      buildStepId(operation.toUpperCase, leftId, rightId)
    }
    stepIds += id
    val params = (buildParameter(if (right.isDefined) "left" else "dataReference", "@" + leftId) +:
      parameters.getOrElse(List())) ++ rightId.map(id => buildParameter("right", "@" + id))
    val step = PipelineStep(
      Some(id),
      `type` = Some("pipeline"),
      params = Some(params),
      value = Some("STEP"),
      engineMeta = Some(EngineMeta(Some("QueryingSteps." + operation)))
    )
    aggregateResult(right.map(aggregateResult(left, _)).getOrElse(left), List(step))
  }

  override protected def buildParameter(name: String, ctx: RuleContext): Parameter = {
    buildParameter(name, getText(ctx), Some("expression"))
  }

  override protected def buildParameter(name: String, ctx: List[RuleContext]): Parameter = {
    Parameter(Some("expression"),
      Some(name.replaceAll("[^a-zA-Z0-9]", "")),
      required = Some(true),
      value = Some(ctx.map(getText))
    )
  }

  override protected def buildParameter(name: String, text: String, typeHint: Option[String] = None): Parameter = {
    Parameter(typeHint.orElse(Some("text")),
      Some(name.replaceAll("[^a-zA-Z0-9]", "")),
      required = Some(true),
      value = Some(text)
    )
  }
}

class DataReferenceBuilder(override val tokenStream: TokenStream, pipelineContext: PipelineContext) extends SqlParser[DataReference[_], (String, Any)] {

  override protected def buildAndChain(operation: String,
                                       left: DataReference[_],
                                       right: Option[DataReference[_]],
                                       parameters: Option[List[(String, Any)]]): DataReference[_] = {
    val paramMap = parameters.map(_.toMap).getOrElse(Map.empty[String, Any])
    operation.toUpperCase match {
      case "JOIN" =>
        val condition = paramMap.get("condition").map(_.asInstanceOf[Expression])
        val using = paramMap.get("using").map(_.asInstanceOf[List[Expression]])
        val jt = paramMap.get("joinType").mkString
        applyQueryOperation(left, Join(right.get, jt, condition, using), pipelineContext)
      case op if !op.contains('.') =>
        val args: Array[AnyRef] = parameters.map(_.map(_._2.asInstanceOf[AnyRef]).toArray).getOrElse(Array())
        val className = s"com.acxiom.metalus.sql.${operation.capitalize}"
        val qo = ReflectionUtils.fastLoadClass(className, args)
        applyQueryOperation(left, qo.asInstanceOf[QueryOperator], pipelineContext)
      case _ =>
        val args: Array[AnyRef] = parameters.map(_.map(_._2.asInstanceOf[AnyRef]).toArray).getOrElse(Array())
        val tmp = operation.split('.')
        val pkg = tmp.dropRight(1)
        val className = tmp.last.capitalize
        val qo = ReflectionUtils.fastLoadClass(s"${pkg.mkString(".")}.$className", args)
        applyQueryOperation(left, qo.asInstanceOf[QueryOperator], pipelineContext)
    }
  }

  override protected def buildParameter(name: String, ctx: RuleContext): (String, Any) =
    name -> Expression(getText(ctx))

  override protected def buildParameter(name: String, ctx: List[RuleContext]): (String, Any) =
    name -> ctx.map(r => Expression(getText(r)))

  override protected def buildParameter(name: String, text: String, typeHint: Option[String]): (String, Any) =
    name -> (typeHint.getOrElse("text").toLowerCase match {
      case "byte" => text.toByte
      case "short" => text.toShort
      case "integer" | "int" => text.toInt
      case "long" => text.toLong
      case "float" => text.toFloat
      case "double" => text.toDouble
      case "boolean" => text.toBoolean
      case "text" | "string" => text
    })

  override def visitDataReference(ctx: DataReferenceContext): DataReference[_] = {
    Option(ctx.step()).map(visitStep).getOrElse {
      pipelineContext.parameterMapper
        .mapParameter(Parameter(Some("text"), Some("dataReference"), value = Some(ctx.mapping().getText)), pipelineContext)
        .asInstanceOf[DataReference[_]]
    }
  }

  override def visitStep(ctx: StepContext): DataReference[_] = {
    val (pkg, obj, method) = ctx.IDENTIFIER().asScala.map(_.getText).toList match {
      case List(m) => (None, "QueryingSteps.", m)
      case List(obj, m) => (None, obj, m)
      case l =>
        val (pkgList, stepList) = l.splitAt(l.size - 2)
        (Some(pkgList.mkString(".")), stepList.head, stepList.last)
    }
    val params = ctx.stepParam().asScala.map {
      case paramCtx if Option(paramCtx.stepParamName).exists(_.getText.nonEmpty) =>
        Parameter(Some("text"),
          Some(paramCtx.stepParamName.getText),
          required = Some(true),
          value = Some(paramCtx.stepValue().getText)
        )
      case paramCtx =>
        val text = paramCtx.stepValue().getText
        Parameter(Some("text"),
          Some(text.replaceFirst("[!$#@]", "")),
          required = Some(true),
          value = Some(text)
        )
    }.toList
    val step = PipelineStep(
      Some("id"),
      `type` = Some("pipeline"),
      params = Some(params),
      value = Some("STEP"),
      engineMeta = Some(EngineMeta(Some(obj + "." + method), pkg))
    )
    val parameters = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)
    val res = ReflectionUtils.processStep(step, parameters, pipelineContext).primaryReturn
      .collect{case dr: DataReference[_] => dr}
    if (res.isEmpty) {
      throw PipelineException( message = Some(""), pipelineProgress = pipelineContext.currentStateInfo)
    }
    res.get
  }

  override def aggregateResult(aggregate: DataReference[_], nextResult: DataReference[_]): DataReference[_] =
    (Option(aggregate), Option(nextResult)) match {
      case (None, Some(agg)) => agg
      case (Some(agg), None) => agg
      case (None, None) => None.orNull
    }

}
