package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.sql.parser.ExpressionParser.KeywordExecutor
import com.acxiom.metalus.{Parameter, PipelineContext, PipelineStateKey}
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

  private val default: KeywordExecutor = {case _ => None}

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

  private lazy val applyMethod = pipelineContext.getGlobal("extractMethodsEnabled").asInstanceOf[Option[Boolean]]

  override def visitStringLit(ctx: StringLitContext): Option[Any] = Some(ctx.getText.stripPrefix("'").stripSuffix("'"))
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Option[Any] = Some(ctx.getText.toInt)
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Option[Any] = Some(ctx.getText.toDouble)
  override def visitBooleanLit(ctx: BooleanLitContext): Option[Any] = Some(ctx.getText.toLowerCase.toBoolean)

  override def visitReservedVal(ctx: ReservedValContext): Option[Any] =
    keywordExecutor.lift(ctx.getText).flatten

  override def visitMapping(ctx: MappingContext): Option[Any] = ctx.symbol.getType match {
    case MExprParser.GLOBAL => getGlobal(ctx.key.IDENTIFIER().asScala.toList)
    case MExprParser.PERCENT => getCredential(ctx.key.IDENTIFIER().asScala.toList)
    case MExprParser.STEP_RETURN => None
    case MExprParser.SECONDARY_RETURN => None
    case MExprParser.PIPELINE => pipelineContext.pipelineManager.getPipeline(ctx.key.IDENTIFIER().asScala.toList.head.getText)
    case MExprParser.PARAMETER => getPipelineParameter(ctx.key.IDENTIFIER().asScala.toList)
    case MExprParser.R_PARAMETER => getPipelineParameter(ctx.key.IDENTIFIER().asScala.toList).map {
      case s: String => ExpressionParser.parse(s, pipelineContext)
      case v => v
    }

  }

  override def visitListValue(ctx: ListValueContext): Option[Any] = Some(ctx.stepValue().asScala.flatMap(visit).toList)

  override def visitStringConcat(ctx: StringConcatContext): Option[Any] = {
    List(ctx.left.accept(this), ctx.right.accept(this))
      .flatten.reduceOption(_.toString + _.toString)
  }

//  override def visitStepConcat(ctx: StepConcatContext): Option[Any] = visit(ctx.left) orElse visit(ctx.right)

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

  private def getGlobal(identifiers: List[TerminalNode]): Option[Any] = {
    val key = identifiers.head.getText
    val extractPath = identifiers.drop(1).map(_.getText).mkString(".")
    pipelineContext.getGlobal(key).flatMap{
        case s: String if pipelineContext.isGlobalLink(key) =>
          val globals = pipelineContext.globals.getOrElse(Map[String, Any]())
          ExpressionParser.parse(s, pipelineContext.copy(globals = Some(globals - "GlobalLinks")))
        case o: Option[_] => o
        case default => Some(default)
      }.map(ReflectionUtils.extractField(_, extractPath, applyMethod = applyMethod))
  }

  private def getCredential(identifiers: List[TerminalNode]): Option[Any] = {
    val credentialName = identifiers.head.getText
    val extractPath = identifiers.drop(1).map(_.getText).mkString(".")
    pipelineContext.credentialProvider
      .flatMap(_.getNamedCredential(credentialName))
      .map(ReflectionUtils.extractField(_, extractPath, applyMethod = applyMethod))
  }

  private def getPipelineParameter(identifiers: List[TerminalNode]): Option[Any] = {
    val paths = identifiers.map(_.getText)
    val localPipelineKey = pipelineContext.currentStateInfo.get.copy(stepId = None, forkData = None).key
    pipelineContext.getParameterByPipelineKey(localPipelineKey, paths.head)
      .map(ReflectionUtils.extractField(_, paths.drop(1).mkString("."), applyMethod = applyMethod))
      .orElse {
        val keyList = pipelineContext.parameters.map(_.pipelineKey.copy(stepId = None, forkData = None))
        val (key, drop, _) = determineParameterKey(paths, keyList, "")
        val sliced = paths.drop(drop)
        pipelineContext.getParameterByPipelineKey(key, sliced.headOption.mkString)
          .map(ReflectionUtils.extractField(_, sliced.drop(1).mkString("."), applyMethod = applyMethod))
      }
  }

  @tailrec
  private def determineParameterKey(paths: List[String], keys: List[PipelineStateKey],
                                    key: String, index: Int = 0, fork: Boolean = false): (String, Int, Boolean) = {
    // See if we have reached the last token
    if (index >= paths.length) {
      (key, index, fork)
    } else {
      val token = paths(index)
      // Make sure that key has a value else use the token
      val currentKey = if (key.nonEmpty) {
        key + "." + token
      } else {
        token
      }
      val nextIndex = index + 1
      val matchedKey = keys.find(k => k.key == currentKey || (k.forkData.isDefined && k.copy(forkData = None).key == currentKey))
      if (matchedKey.isEmpty) {
        determineParameterKey(paths, keys, currentKey, nextIndex, fork)
      } else {
        (currentKey, nextIndex, matchedKey.get.forkData.isDefined)
      }
    }
  }

}
