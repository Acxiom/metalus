package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.sql.parser.ExpressionParser.KeywordExecutor
import com.acxiom.metalus.{MappingResolver, Parameter, PipelineContext, PipelineStateKey}
import com.acxiom.metalus.sql.parser.MExprParser._
import com.acxiom.metalus.utils.ReflectionUtils
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.TerminalNode

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object ExpressionParser {

  type KeywordExecutor = PartialFunction[String, Option[Any]]

  val VALUE = "VALUE"
  val STEP = "STEP"

  private val default: KeywordExecutor = {
    case _ => None
  }

  def parse(expr: String, pipelineContext: PipelineContext)(implicit keywordExecutor: KeywordExecutor = default): Option[Any] = {
    val cs = UpperCaseCharStream.fromString(expr)
    val lexer = new MSqlLexer(cs)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val cts = new CommonTokenStream(lexer)
    val parser = new MExprParser(cts)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    new ExpressionParser(pipelineContext, keywordExecutor).visit(parser.singleStepExpression())
  }

}

class ExpressionParser(pipelineContext: PipelineContext, keywordExecutor: KeywordExecutor) extends MExprParserBaseVisitor[Option[Any]] {

  override def visitStringLit(ctx: StringLitContext): Option[Any] = Some(ctx.getText.stripPrefix("'").stripSuffix("'"))

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Option[Any] = Some(ctx.getText.toInt)

  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Option[Any] = Some(ctx.getText.toDouble)

  override def visitBooleanLit(ctx: BooleanLitContext): Option[Any] = Some(ctx.getText.toLowerCase.toBoolean)

  override def visitReservedVal(ctx: ReservedValContext): Option[Any] =
    keywordExecutor.lift(ctx.getText).flatten

  override def visitMapping(ctx: MappingContext): Option[Any] = ctx.symbol.getType match {
    case MExprParser.GLOBAL =>
      MappingResolver.getGlobalParameter(ctx.key.IDENTIFIER().asScala.toList.mkString("."), pipelineContext,
        Some(ExpressionParser.parse(_, _)(keywordExecutor)))
    case MExprParser.PERCENT =>
    MappingResolver.getCredential(ctx.key.IDENTIFIER().asScala.toList.mkString("."), pipelineContext)
    case MExprParser.STEP_RETURN =>
      MappingResolver.getStepResponse(ctx.key.IDENTIFIER().asScala.toList.mkString("."), secondary = false, pipelineContext)
    case MExprParser.SECONDARY_RETURN =>
      MappingResolver.getStepResponse(ctx.key.IDENTIFIER().asScala.toList.mkString("."), secondary = true, pipelineContext)
    case MExprParser.PIPELINE => pipelineContext.pipelineManager.getPipeline(ctx.key.IDENTIFIER().asScala.toList.head.getText)
    case MExprParser.PARAMETER =>
      MappingResolver.getPipelineParameter(ctx.key.IDENTIFIER().asScala.toList.mkString("."), pipelineContext, None)
    case MExprParser.R_PARAMETER =>
      MappingResolver.getPipelineParameter(ctx.key.IDENTIFIER().asScala.toList.mkString("."), pipelineContext,
        Some(ExpressionParser.parse(_, pipelineContext)(keywordExecutor)))

  }

  override def visitListValue(ctx: ListValueContext): Option[Any] =
    Some(Option(ctx.stepValue()).map(_.asScala.flatMap(visit).toList).getOrElse(List()))

//  override def visitMapValue(ctx: MapValueContext): Option[Any] = Some {
//    Option(ctx.mapParam()).map(_.asScala.map { pctx =>
//      pctx.string().getText -> visit(pctx.stepValue())
//    }.collect { case (s, Some(v)) => s -> v })
//      .getOrElse(Map[String, Any]())
//  }

  override def visitStringConcat(ctx: StringConcatContext): Option[Any] = {
    List(ctx.left.accept(this), ctx.right.accept(this))
      .flatten.reduceOption(_.toString + _.toString)
  }

  // need this for short circuiting logic
  override def visitStepConcat(ctx: StepConcatContext): Option[Any] = visit(ctx.left) orElse visit(ctx.right)

  override def visitBooleanNot(ctx: BooleanNotContext): Option[Any] = Some(!toBoolean(ctx.stepExpression()))

  override def visitBooleanExpr(ctx: BooleanExprContext): Option[Any] = ctx.operator.getType match {
    case MExprParser.AND => Some(toBoolean(ctx.left) && toBoolean(ctx.right))
    case MExprParser.OR => Some(toBoolean(ctx.left) || toBoolean(ctx.right))
    case MExprParser.EQ => Some(visit(ctx.left).exists(visit(ctx.right).contains))
    case MExprParser.NEQ => Some(!visit(ctx.left).exists(visit(ctx.right).contains))
  }

  override def visitIfStatement(ctx: IfStatementContext): Option[Any] = if (toBoolean(ctx.ifExpr)) {
    visit(ctx.`then`)
  } else {
    visit(ctx.elseExpr)
  }

  override def defaultResult(): Option[Any] = None

  override def aggregateResult(aggregate: Option[Any], nextResult: Option[Any]): Option[Any] =
    aggregate orElse nextResult

  private def toBoolean(ctx: ParserRuleContext): Boolean = ctx.accept(this) match {
    case Some(b: Boolean) => b
    case None => false
    case _ => true
  }

}
