package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.api.HttpRestClient
import com.acxiom.metalus.sql.Row
import com.acxiom.metalus.sql.parser.ExpressionArithmetic.isNumber
import com.acxiom.metalus.sql.parser.ExpressionParser.KeywordExecutor
import com.acxiom.metalus.{MappingResolver, PipelineContext, PipelineException}
import com.acxiom.metalus.sql.parser.MExprParser._
import com.acxiom.metalus.utils.ReflectionUtils
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.RuleNode
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.Serialization
import org.slf4j.LoggerFactory

import java.math.BigInteger
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.math.Numeric.{BigDecimalAsIfIntegral, DoubleAsIfIntegral, FloatAsIfIntegral}
import scala.math.ScalaNumber
import scala.util.{Failure, Success, Try}
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
    val failOnException = pipelineContext.getGlobal("strictExpressions")
      .exists(_.toString.toLowerCase == "true")
    new ExpressionParser(pipelineContext, keywordExecutor, None, failOnException).visit(tree)
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
class ExpressionParser(pipelineContext: PipelineContext, keywordExecutor: KeywordExecutor, derefObj: Option[Any],
                       failOnError: Boolean) extends MExprParserBaseVisitor[Option[Any]] {

  private val logger = LoggerFactory.getLogger(getClass)

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
    } match {
      case Some(None) => None
      case o => o
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
      new ExpressionParser(pipelineContext, keywordExecutor orElse { case `label` => Some(v) }, derefObj, failOnError)
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
      case (t: Traversable[Any], MExprParser.TO_MAP) if t.headOption.forall(_.isInstanceOf[(Any, Any)])=>
        t.asInstanceOf[Traversable[(Any, Any)]].toMap
      case (a: Array[Any], MExprParser.TO_ARRAY) => a
      case (a: Array[Any], MExprParser.TO_LIST) => a.toList
      case (a: Array[Any], MExprParser.TO_MAP) if a.headOption.forall(_.isInstanceOf[(Any, Any)]) =>
        a.asInstanceOf[Array[(Any, Any)]].toMap
    } orElse {
      ctx.name.getType match {
        case MExprParser.TO_ARRAY => Some(value.toArray)
        case MExprParser.TO_LIST => Some(value.toList)
        case MExprParser.TO_MAP if value.forall(_.isInstanceOf[(Any, Any)]) =>
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
    case m: Map[Any, _] => visit(ctx.stepExpression()).flatMap(a => m.get(a).map(unwrap(_)))
    case r: Row => visit(ctx.stepExpression()).flatMap {
      case i: Int => r.get(i)
      case l: Long => r.get(l.toInt)
      case s: Short => r.get(s.toInt)
      case a => r.get(a.toString)
    }
    case unsupported if failOnError =>
      throw PipelineException(
        message = Some(s"Unable to execute expression [${ctx.getText}], object: [${unsupported.toString}] is not a collection."),
        pipelineProgress = pipelineContext.currentStateInfo)
    case unsupported =>
      logger.warn(s"Unable to execute expression [${ctx.getText}], object: [${unsupported.toString}] is not a collection.")
      None
  }

  override def visitDereference(ctx: DereferenceContext): Option[Any] = visit(ctx.left).flatMap { v =>
    new ExpressionParser(pipelineContext, keywordExecutor, Some(v), failOnError).visit(ctx.right)
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

  override def visitUnaryArithmetic(ctx: UnaryArithmeticContext): Option[Any] = {
    val op = ctx.operator.getText
    visit(ctx.stepExpression()).flatMap {
      case n if op == "-" && isNumber(n) => ExpressionArithmetic(op, n)
      case n if op == "+" && isNumber(n) => Some(n)
      case unsupported if failOnError =>
        throw PipelineException(
          message = Some(s"Unary operator [$op] does not support object: [${unsupported.toString}]."),
          pipelineProgress = pipelineContext.currentStateInfo)
      case unsupported =>
        logger.warn(s"Unary operator [$op] does not support object: [${unsupported.toString}].")
        None
    }
  }

  override def visitArithmetic(ctx: ArithmeticContext): Option[Any] = {
    val op = ctx.operator.getText
    val l = List(visit(ctx.left), visit(ctx.right)).flatten
    val func: (Any, Any) => Option[Any] = {
      case (left, right) if isNumber(left) && isNumber(right) => ExpressionArithmetic(left, op, right)
      case (left: Traversable[Any], right: Traversable[Any]) if op == "++" => Some(left ++ right)
      case (left: Array[Any], right: Array[Any]) if op == "++" => Some(left ++ right)
      case (left, right) if op == "+" => Some(left.toString + right.toString) // treat all else as string concatenation
      case _ => None // unsupported operation
    }
    l.reduceOption((l, r) => unwrap(func(l, r), recursive = false))
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
    val args = Option(ctx.stepExpression())
      .map(_.asScala.map(visit).map(unwrap(_, recursive = false)).map(_.asInstanceOf[AnyRef]).toArray)
      .getOrElse(Array())
    ReflectionUtils.fastLoadClass(ctx.stepIdentifier().getText, args)
  } match {
    case Failure(err) if failOnError =>
      throw PipelineException(message = Some(s"Failed to instantiate object: ${ctx.stepIdentifier().getText}"),
        cause = err, pipelineProgress = pipelineContext.currentStateInfo)
    case Failure(err) =>
      logger.error(s"Exception caught attempting to instantiate object: ${ctx.stepIdentifier().getText}.", err)
      None
    case Success(res) => Some(res)
  }

  override def visitRestCall(ctx: RestCallContext): Option[Any] =
    ExpressionRestCall(ctx.httpMethod.getType, visit(ctx.url).mkString, Option(ctx.body).flatMap(visit),
      Option(ctx.headers).flatMap(visit), failOnError, pipelineContext)

  override def visitMathFunction(ctx: MathFunctionContext): Option[Any] = {
    val name = visit(ctx.identifier()).mkString.toLowerCase
    val arguments = ctx.stepExpression().asScala.flatMap(visit).toList
    val res = (name, arguments) match {
      case ("abs", List(n))=> ExpressionArithmetic("abs", n)
      case (n@("ceil"|"floor"|"round"), List(num)) if isNumber(num) =>
        val d = toDouble(num)
        Some(n match {
          case "ceil" => d.ceil
          case "floor" => d.floor
          case "round" => d.round
        })
      case (n@("max"|"min"), List(left, right)) => ExpressionArithmetic(left, n, right)
      case (n@("max"|"min"), List(num)) if isNumber(num) => Some(num) // special case to handle where one side is None
      case _ => None
    }
    res match {
      case None if failOnError =>
        throw PipelineException(message = Some(s"No function found for call: [$name(${arguments.mkString(", ")})"),
          pipelineProgress = pipelineContext.currentStateInfo)
      case None =>
        logger.warn(s"No function found for call: [$name(${arguments.mkString(", ")})")
        None
      case s => s
    }
  }

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

  private def toDouble(a: Any): Double = a match {
    case d: Double => d
    case n: Number => n.doubleValue()
    case bi: BigInt => bi.doubleValue()
    case bd: BigDecimal => bd.doubleValue()
    case s => s.toString.toDouble
  }

  @tailrec
  private def unwrap(value: Any, recursive: Boolean = true): Any = value match {
    case Some(v) if recursive => unwrap(v)
    case Some(v) => v
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

private[parser] object ExpressionArithmetic {

  private val BYTE = 0
  private val SHORT = 1
  private val INT = 2
  private val LONG = 3
  private val BIG_INT = 4
  private val FLOAT = 5
  private val DOUBLE = 6
  private val BIG_DECIMAL = 7
  private val NAN = Int.MaxValue

  // scalastyle:off cyclomatic.complexity
  def apply(op: String, right: Any): Option[Any] = typeOrdering(right) match {
    case BYTE => Some(unary[Byte](op, right.asInstanceOf[Number].byteValue()))
    case SHORT => Some(unary[Short](op, right.asInstanceOf[Number].shortValue()))
    case INT => Some(unary[Int](op, right.asInstanceOf[Number] intValue()))
    case LONG => Some(unary[Long](op, right.asInstanceOf[Number].longValue()))
    case BIG_INT => Some(unary[BigInt](op, toBigInt(right)))
    case FLOAT => Some(unary[Float](op, right.asInstanceOf[Number].floatValue()))
    case DOUBLE => Some(unary[Double](op, right.asInstanceOf[Number].doubleValue()))
    case BIG_DECIMAL => Some(unary[BigDecimal](op, toBigDecimal(right)))
    case NAN => None
  }

  // scalastyle:off cyclomatic.complexity
  def apply(left: Any, op: String, right: Any): Option[Any] = {
    val bound = (typeOrdering(left), typeOrdering(right)) match {
      case (BIG_INT, r) if r > BIG_INT => BIG_DECIMAL
      case (l, r) => math.max(l, r)
    }
    bound match {
      case BYTE => Some(arithmetic[Byte](left.asInstanceOf[Number].byteValue(), op, right.asInstanceOf[Number].byteValue()))
      case SHORT => Some(arithmetic[Short](left.asInstanceOf[Number].shortValue(), op, right.asInstanceOf[Number].shortValue()))
      case INT => Some(arithmetic[Int](left.asInstanceOf[Number].intValue(), op, right.asInstanceOf[Number]intValue()))
      case LONG => Some(arithmetic[Long](left.asInstanceOf[Number].longValue(), op, right.asInstanceOf[Number].longValue()))
      case BIG_INT => Some(arithmetic[BigInt](toBigInt(left), op, toBigInt(right)))
      // % needs integral instead of fractional, so specifying that manually for float, double and bigDecimal
      case FLOAT if op == "%"=>
        Some(arithmetic[Float](left.asInstanceOf[Number].floatValue(), op, right.asInstanceOf[Number].floatValue())(FloatAsIfIntegral))
      case DOUBLE if op == "%" =>
        Some(arithmetic[Double](left.asInstanceOf[Number].doubleValue(), op, right.asInstanceOf[Number].doubleValue())(DoubleAsIfIntegral))
      case BIG_DECIMAL if op == "%" =>
        Some(arithmetic[BigDecimal](toBigDecimal(left), op, toBigDecimal(right))(BigDecimalAsIfIntegral))
      case FLOAT => Some(arithmetic[Float](left.asInstanceOf[Number].floatValue(), op, right.asInstanceOf[Number].floatValue()))
      case DOUBLE => Some(arithmetic[Double](left.asInstanceOf[Number].doubleValue(), op, right.asInstanceOf[Number].doubleValue()))
      case BIG_DECIMAL => Some(arithmetic[BigDecimal](toBigDecimal(left), op, toBigDecimal(right)))
      case NAN => None
    }
  }

  def toBigInt(any: Any): BigInt = any match {
    case bi: BigInt => bi
    case bi: BigInteger => BigInt(bi)
    case n: Number => BigInt(n.longValue())
    case s if s.toString.forall(c => c.isDigit) => BigInt(s.toString)
  }

  def toBigDecimal(any: Any): BigDecimal = any match {
    case bd: BigDecimal => bd
    case bd: java.math.BigDecimal => BigDecimal(bd)
    case n: Number => BigDecimal(n.doubleValue())
    case s if s.toString.forall(c => c.isDigit || c == '.') => BigDecimal(s.toString)
  }

  def isNumber(any: Any): Boolean = any match {
    case _: Number => true
    case _: BigInt | _: BigDecimal | _: BigInteger | _: java.math.BigDecimal => true
    case _ => false
  }

  private def typeOrdering(n: Any): Int = n match {
    case _: Byte | _: java.lang.Byte => BYTE
    case _: Short | _: java.lang.Short => SHORT
    case _: Int | _: Integer => INT
    case _: Long | _: java.lang.Long => LONG
    case _: BigInt | _: BigInteger => BIG_INT
    case _: Float | _: java.lang.Float => FLOAT
    case _: Double | _: java.lang.Double => DOUBLE
    case _: BigDecimal | _: java.math.BigDecimal => BIG_DECIMAL
    case _ => NAN
  }

  private def unary[T](op: String, right: T)(implicit num: Numeric[T]): T = (op, num) match {
    case ("-", n) => n.negate(right)
    case ("abs", n) => n.abs(right)
  }

  private def arithmetic[T](left: T, op: String, right: T)(implicit num: Numeric[T]): T = (op, num) match {
    case ("+", n) => n.plus(left, right)
    case ("-", n) => n.minus(left, right)
    case ("*", n) => n.times(left, right)
    case ("/", n: Fractional[T]) => n.div(left, right)
    case ("/", n: Integral[T]) => n.quot(left, right)
    case ("%", n: Integral[T]) => n.rem(left, right)
    case ("max", n) => n.max(left, right)
    case ("min", n) => n.min(left, right)
  }
}

private[parser] object ExpressionRestCall {

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val formats: Formats = DefaultFormats
  def apply(method: Int, url: String, body: Option[Any], headers: Option[Any],
            failOnError: Boolean, pipelineContext: PipelineContext): Option[Any] = Try {
    val client = HttpRestClient(url)
    val finalHeaders = getHeaders(headers)
    val useJson = body.exists(b => b.isInstanceOf[Map[_, _]] || b.isInstanceOf[List[_]])
    method match {
      case MExprParser.GET =>
        client.getStringContent("", finalHeaders)
      case MExprParser.PUT if useJson =>
        client.putJsonContent("", Serialization.write(body.get), finalHeaders)
      case MExprParser.POST if useJson=>
        client.postJsonContent("", Serialization.write(body.get), finalHeaders)
      case MExprParser.PATCH if useJson =>
        client.patchJsonContent("", Serialization.write(body.get), finalHeaders)
      case MExprParser.PUT =>
        client.putStringContent("", body.mkString, headers = finalHeaders)
      case MExprParser.POST =>
        client.postStringContent("", body.mkString, headers = finalHeaders)
      case MExprParser.PATCH =>
        client.patchStringContent("", body.mkString, headers = finalHeaders)
      case MExprParser.DELETE =>
        client.delete("", finalHeaders)
    }
  } match {
    case Failure(err) if failOnError =>
      throw PipelineException(message = Some(s"Failed to make call to api: $url"),
        cause = err, pipelineProgress = pipelineContext.currentStateInfo)
    case Failure(err) =>
      logger.error(s"Exception caught making call to api: $url.", err)
      None
    case Success(res) => Some(res)
  }

  private def getHeaders(headers: Option[Any]): Option[Map[String, String]] = headers.collect {
    case m: Map[_, _] => m.collect {
      case (k, Some(v)) => k.toString -> v.toString
      case (k, v) if !v.isInstanceOf[Option[_]] => k.toString -> v.toString
    }
    case l: Traversable[_] => l.collect {
      case Tuple2(k, Some(v)) => k.toString -> v.toString
      case Tuple2(k, v) if !v.isInstanceOf[Option[_]] => k.toString -> v.toString
    }.toMap
  }.filter(_.nonEmpty)
}
