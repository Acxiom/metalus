package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.sql.Row
import com.acxiom.metalus.sql.parser.ExpressionParser.KeywordExecutor
import com.acxiom.metalus.{MappingResolver, PipelineContext}
import com.acxiom.metalus.sql.parser.MExprParser._
import com.acxiom.metalus.utils.ReflectionUtils
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.RuleNode

import java.math.BigInteger
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.math.ScalaNumber
import scala.util.Try
import scala.util.control.NonFatal

object ExpressionParser {

  type KeywordExecutor = PartialFunction[String, Option[Any]]

  val VALUE = "VALUE"
  val STEP = "STEP"

  private val default: KeywordExecutor = {
    case _ => None
  }

  def parse(expr: String, pipelineContext: PipelineContext)(implicit keywordExecutor: KeywordExecutor = default): Option[Any] = {
    val tree = parseToTree(expr)
    new ExpressionParser(pipelineContext, keywordExecutor, None).visit(tree)
  }

  private def parseToTree(expr: String): SingleStepExpressionContext = {
    val cs = UpperCaseCharStream.fromString(expr)
    val lexer = new MSqlLexer(cs)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val cts = new CommonTokenStream(lexer)
    val parser = new MExprParser(cts)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
    try {
      parser.singleStepExpression()
    } catch {
      case NonFatal(_) =>
        lexer.reset()
        parser.reset()
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        parser.singleStepExpression()
    }
  }

}

//noinspection ScalaStyle
class ExpressionParser(pipelineContext: PipelineContext, keywordExecutor: KeywordExecutor, derefObj: Option[Any]) extends MExprParserBaseVisitor[Option[Any]] {

  override def visitStringLit(ctx: StringLitContext): Option[Any] = Some(ctx.getText.stripPrefix("'").stripSuffix("'"))

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Option[Any] = Some(ctx.getText.toInt)

  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Option[Any] = Some(ctx.getText.toDouble)

  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Option[Any] = Some(ctx.getText.toDouble)

  override def visitBooleanLit(ctx: BooleanLitContext): Option[Any] = Some(ctx.getText.toLowerCase.toBoolean)

  override def visitVariableAccess(ctx: VariableAccessContext): Option[Any] = {
    derefObj.map{ obj =>
      val path = ctx.stepIdentifier().identifier().asScala.flatMap(visit).mkString(".")
      ReflectionUtils.extractField(obj, path)
    } orElse {
      val idents = Option(ctx.stepIdentifier()).map(_.identifier().asScala.flatMap(visit).map(_.toString))
        .getOrElse(List(ctx.reservedRef().getText))
      keywordExecutor.lift(idents.head).flatten.map{ obj =>
        ReflectionUtils.extractField(obj, idents.drop(1).mkString("."))
      }
    }
  }

  override def visitMapping(ctx: MappingContext): Option[Any] = {
    val ident = ctx.key.identifier().asScala.flatMap(visit).mkString(".")
    ctx.symbol.getType match {
      case MExprParser.GLOBAL =>
        MappingResolver.getGlobalParameter(ident, pipelineContext,
          Some(ExpressionParser.parse(_, _)(keywordExecutor)))
      case MExprParser.PERCENT =>
        MappingResolver.getCredential(ident, pipelineContext)
      case MExprParser.STEP_RETURN =>
        MappingResolver.getStepResponse(ident, secondary = false, pipelineContext)
      case MExprParser.SECONDARY_RETURN =>
        MappingResolver.getStepResponse(ident, secondary = true, pipelineContext)
      case MExprParser.AMPERSAND => pipelineContext.pipelineManager.getPipeline(ident.split('.').head)
      case MExprParser.PARAMETER =>
        MappingResolver.getPipelineParameter(ident, pipelineContext, None)
      case MExprParser.R_PARAMETER =>
        MappingResolver.getPipelineParameter(ident, pipelineContext,
          Some(ExpressionParser.parse(_, pipelineContext)(keywordExecutor)))
    }
  }

  override def visitUnquotedIdentifier(ctx: UnquotedIdentifierContext): Option[Any] = Some(ctx.getText)
  override def visitQuotedIdentifier(ctx: QuotedIdentifierContext): Option[Any] = Some(ctx.getText.drop(1).dropRight(1))
  override def visitBackQuotedIdentifier(ctx: BackQuotedIdentifierContext): Option[Any] = Some(ctx.getText.drop(1).dropRight(1))

  override def visitLambda(ctx: LambdaContext): Option[Any] = {
    val value = visit(ctx.stepValueExpression())
    val label = visit(ctx.label).mkString
    val func: Any => Option[Any] = { v =>
      new ExpressionParser(pipelineContext, keywordExecutor orElse { case `label` => Some(v) }, None)
        .visit(ctx.stepExpression())
    }
    value.collect {
      case t: Traversable[Any] => applyTraversableLambda(t, ctx.name)(func)
      case a: Array[Any] => applyArrayLambda(a, ctx.name)(func)
    }.flatten orElse {
      applyTraversableLambda(value, ctx.name)(func)
    }
  }

  override def visitToCollection(ctx: ToCollectionContext): Option[Any] = {
    val value = visit(ctx.stepValueExpression())
    value.map((_, ctx.name.getType)).collect{
      case (t: Traversable[Any], MExprParser.TO_ARRAY) => t.toArray
      case (t: Traversable[Any], MExprParser.TO_LIST) => t.toList
      case (t: Traversable[Any], MExprParser.TO_MAP) if t.headOption.exists(_.isInstanceOf[(Any, Any)])=>
        t.asInstanceOf[Traversable[(Any, Any)]].toMap
      case (a: Array[Any], MExprParser.TO_ARRAY) => a
      case (a: Array[Any], MExprParser.TO_LIST) => a.toList
      case (a: Array[Any], MExprParser.TO_MAP) if a.headOption.exists(_.isInstanceOf[(Any, Any)]) =>
        a.asInstanceOf[Array[(Any, Any)]].toMap
    } orElse {
      ctx.name.getType match {
        case MExprParser.TO_ARRAY => Some(value.toArray)
        case MExprParser.TO_LIST => Some(value.toList)
        case MExprParser.TO_MAP if value.exists(_.isInstanceOf[(Any, Any)]) =>
          Some(value.asInstanceOf[Option[(Any, Any)]].toMap)
      }
    }
  }

  private def applyTraversableLambda(t: Traversable[Any], functionName: Token)(func: Any => Option[Any]): Option[Any] =
    functionName.getType match {
      case MExprParser.EXISTS => Some(t.exists(v => toBoolean(func(v))))
      case MExprParser.FILTER => Some(t.filter(v => toBoolean(func(v))))
      case MExprParser.FIND => t.find(v => toBoolean(func(v)))
      case MExprParser.MAP => Some(t.flatMap(v => func(v)))
      case MExprParser.REDUCE => t.reduceOption((left, right) => unwrap(func(ReducePair(left, right))))
    }

  private def applyArrayLambda(a: Array[Any], functionName: Token)(func: Any => Option[Any]): Option[Any] =
    functionName.getType match {
      case MExprParser.EXISTS => Some(a.exists(v => toBoolean(func(v))))
      case MExprParser.FILTER => Some(a.filter(v => toBoolean(func(v))))
      case MExprParser.FIND => a.find(v => toBoolean(func(v)))
      case MExprParser.MAP => Some(a.flatMap(v => func(v)))
      case MExprParser.REDUCE => a.reduceOption((left, right) => unwrap(func(ReducePair(left, right))))
    }

  override def visitCollectionAccess(ctx: CollectionAccessContext): Option[Any] = visit(ctx.stepValueExpression()).flatMap {
    case a: Array[_] => toIntOption(ctx.stepExpression()).map(a.apply)
    case s: Seq[_] => toIntOption(ctx.stepExpression()).map(s.apply)
    case m: Map[Any, _] => visit(ctx.stepExpression()).flatMap(a => m.get(a).map(unwrap))
    case r: Row => visit(ctx.stepExpression()).flatMap {
      case i: Int => r.get(i)
      case l: Long => r.get(l.toInt)
      case s: Short => r.get(s.toInt)
      case a => r.get(a.toString)
    }
  }

  override def visitDereference(ctx: DereferenceContext): Option[Any] = visit(ctx.left).flatMap { v =>
    new ExpressionParser(pipelineContext, keywordExecutor, Some(v)).visit(ctx.right)
  }

  override def visitListValue(ctx: ListValueContext): Option[Any] =
    Some(Option(ctx.stepExpression()).map(_.asScala.flatMap(visit).toList).getOrElse(List()))

  override def visitMapValue(ctx: MapValueContext): Option[Any] = Some {
    Option(ctx.mapParam()).map(_.asScala.map { pctx =>
      pctx.sqlString().getText.stripPrefix("'").stripSuffix("'") -> visit(pctx.stepExpression())
    }.collect { case (s, Some(v)) => s -> v })
      .map(_.toMap)
      .getOrElse(Map.empty[String, Any])
  }

  override def visitArithmetic(ctx: ArithmeticContext): Option[Any] = {
    val op = ctx.operator.getType
    val l = List(visit(ctx.left), visit(ctx.right)).flatten
    val func: (Any, Any) => Any = {
      case (ScalaNumber(left), ScalaNumber(right)) => (left, right, op) match {
        case (l: BigInt, r: BigInt, MExprParser.PLUS) =>
          l + r
        case (l: BigInt, r: BigDecimal, MExprParser.PLUS) => BigDecimal(l) + r
        case (l: BigDecimal, r: BigInt, MExprParser.PLUS) =>  l + BigDecimal(r)
        case (l: BigDecimal, r: BigDecimal, MExprParser.PLUS) => l + r
        case (l: BigInt, r: BigInt, MExprParser.MINUS) => l - r
        case (l: BigInt, r: BigDecimal, MExprParser.MINUS) => BigDecimal(l) - r
        case (l: BigDecimal, r: BigInt, MExprParser.MINUS) => l - BigDecimal(r)
        case (l: BigDecimal, r: BigDecimal, MExprParser.MINUS) => l - r
        case (l: BigInt, r: BigInt, MExprParser.ASTERISK) =>
          l * r
        case (l: BigInt, r: BigDecimal, MExprParser.ASTERISK) => BigDecimal(l) * r
        case (l: BigDecimal, r: BigInt, MExprParser.ASTERISK) => l * BigDecimal(r)
        case (l: BigDecimal, r: BigDecimal, MExprParser.ASTERISK) => l * r
        case (l: BigInt, r: BigInt, MExprParser.SLASH) => l / r
        case (l: BigInt, r: BigDecimal, MExprParser.SLASH) => BigDecimal(l) / r
        case (l: BigDecimal, r: BigInt, MExprParser.SLASH) => l / BigDecimal(r)
        case (l: BigDecimal, r: BigDecimal, MExprParser.SLASH) => l / r
        case (l: BigInt, r: BigInt, MExprParser.PERCENT) => l % r
        case (l: BigInt, r: BigDecimal, MExprParser.PERCENT) => BigDecimal(l) % r
        case (l: BigDecimal, r: BigInt, MExprParser.PERCENT) => l % BigDecimal(r)
        case (l: BigDecimal, r: BigDecimal, MExprParser.PERCENT) => l % r
      }
      case (left, right) => left.toString + right.toString
    }
    l.reduceOption(func)
//    List(ctx.left.accept(this), ctx.right.accept(this))
//      .flatten.reduceOption(_.toString + _.toString)
  }

  // need this for short circuiting logic
  override def visitStepConcat(ctx: StepConcatContext): Option[Any] = visit(ctx.left) orElse visit(ctx.right)

  override def visitBooleanNot(ctx: BooleanNotContext): Option[Any] = Some(!toBoolean(ctx.stepExpression()))

  override def visitBooleanExpr(ctx: BooleanExprContext): Option[Any] = {
    val left = visit(ctx.left)
    val right = visit(ctx.right)
    ctx.operator.getType match {
      case MExprParser.AND => Some(toBoolean(left) && toBoolean(right))
      case MExprParser.OR => Some(toBoolean(left) || toBoolean(right))
      case MExprParser.EQ => Some(left.exists(right.contains))
      case MExprParser.NEQ => Some(!left.exists(right.contains))
      case MExprParser.LT => lessThan(left, right)
      case MExprParser.LTE => lessThan(left, right).map {
        case true => true
        case false => left.exists(right.contains)
      }
      case MExprParser.GT => greaterThan(left, right)
      case MExprParser.GTE => greaterThan(left, right).map {
        case true => true
        case false => left.exists(right.contains)
      }
    }
  }

  private def lessThan(left: Option[Any], right: Option[Any]): Option[Boolean] = Some {
    (left, right) match {
      case (_, None) => false
      case (None, Some(_)) => true
      case (Some(ScalaNumber(l)), Some(ScalaNumber(r))) => l.doubleValue() < r.doubleValue()
      case (Some(l), Some(r)) => l.toString < r.toString
    }
  }

  private def greaterThan(left: Option[Any], right: Option[Any]): Option[Boolean] = Some {
    (left, right) match {
      case (None, _) => false
      case (Some(_), None) => true
      case (Some(ScalaNumber(l)), Some(ScalaNumber(r))) => l.doubleValue() > r.doubleValue()
      case (Some(l), Some(r)) => l.toString > r.toString
    }
  }

  override def visitIfStatement(ctx: IfStatementContext): Option[Any] = if (toBoolean(ctx.ifExpr)) {
    visit(ctx.`then`)
  } else {
    visit(ctx.elseExpr)
  }

  override def visitSomeValue(ctx: SomeValueContext): Option[Any] = Some(visit(ctx.stepExpression()))

  override def visitNoneValue(ctx: NoneValueContext): Option[Any] = None

  override def visitObject(ctx: ObjectContext): Option[Any] = Try{
    val args = Option(ctx.stepValue())
      .map(_.asScala.flatMap(visit).map(_.asInstanceOf[AnyRef]).toArray)
      .getOrElse(Array())
//    ReflectionUtils.loadJavaClass(ctx.stepIdentifier.getText, args)
  }.toOption

  override def defaultResult(): Option[Any] = None

  override def aggregateResult(aggregate: Option[Any], nextResult: Option[Any]): Option[Any] =
    aggregate orElse nextResult

  override def shouldVisitNextChild(node: RuleNode, currentResult: Option[Any]): Boolean = currentResult.isEmpty

  private def toBoolean(ctx: ParserRuleContext): Boolean = toBoolean(visit(ctx))

  private def toBoolean(a: Option[Any]): Boolean = a match {
    case Some(b: Boolean) => b
    case None => false
    case _ => true
  }

  private def toIntOption(ctx: RuleContext): Option[Int] = visit(ctx) map {
    case i: Int => i
    case a => a.toString.toInt
  }

  @tailrec
  private def unwrap(value: Any): Any = value match {
    case Some(v) => unwrap(v)
    case v => v
  }

}

private[parser] final case class ReducePair(left: Any, right: Any)

object ScalaNumber {
  def unapply(a: Option[Any]): Option[ScalaNumber] = toScalaNumber(a)
  def unapply(a: Any): Option[ScalaNumber] = toScalaNumber(Some(a))

  private def toScalaNumber(a: Option[Any]): Option[ScalaNumber] = a collect {
    case s: Short => BigInt(s)
    case i: Int => BigInt(i)
    case l: Long => BigInt(l)
    case bi: BigInteger => BigInt(bi)
    case f: Float => BigDecimal.decimal(f)
    case d: Double => BigDecimal(d)
    case bd: java.math.BigDecimal => BigDecimal(bd)
    case bi: BigInt => bi
    case bd: BigDecimal => bd
  }
}
