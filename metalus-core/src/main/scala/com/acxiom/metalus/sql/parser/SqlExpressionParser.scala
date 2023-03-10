package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.sql._
import com.acxiom.metalus.sql.parser.MSqlParser._
import org.antlr.v4.runtime.{CommonTokenStream, RuleContext}
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}

import scala.jdk.CollectionConverters._

private[metalus] object SqlExpressionParser {
  def parse(expr: String): BaseExpression = {
    val cs = UpperCaseCharStream.fromString(expr)
    val lexer = new MSqlLexer(cs)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val cts = new CommonTokenStream(lexer)
    val parser = new MSqlParser(cts)
    parser.enableTableIdentifiers = true
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    val visitor = new SqlExpressionVisitor(cts)
    visitor.visit(parser.standaloneExpression())
  }
}

// noinspection ScalaStyle
// scalastyle: off
private[metalus] final class SqlExpressionVisitor(tokenStream: CommonTokenStream) extends MSqlParserBaseVisitor[BaseExpression] {

  // LITERAL EXPRESSIONS
  override def visitStringLiteral(ctx: StringLiteralContext): BaseExpression = {
    val text = ctx.getText
    Literal(text.drop(1).dropRight(1), Some(text.take(1)))(text)
  }

  override def visitInterval(ctx: IntervalContext): BaseExpression =
    Interval(ctx.sqlString().getText, ctx.from.getText, Option(ctx.sign).map(_.getText),
      Option(ctx.to).map(_.getText))(getText(ctx))

  override def visitBooleanLiteral(ctx: BooleanLiteralContext): BaseExpression = getLiteral(ctx)
  override def visitNullLiteral(ctx: NullLiteralContext): BaseExpression = getLiteral(ctx)
  override def visitNumericLiteral(ctx: NumericLiteralContext): BaseExpression = getLiteral(ctx)

  // IDENTIFIER EXPRESSIONS
  override def visitUnquotedIdentifier(ctx: UnquotedIdentifierContext): BaseExpression =
    Identifier(ctx.getText, None, None)(ctx.getText)
  override def visitQuotedIdentifier(ctx: QuotedIdentifierContext): BaseExpression =
    Identifier(ctx.getText.drop(1).dropRight(1), None, Some(ctx.getText.take(1)))(ctx.getText)
  override def visitBackQuotedIdentifier(ctx: BackQuotedIdentifierContext): BaseExpression =
    Identifier(ctx.getText.drop(1).dropRight(1), None, Some(ctx.getText.take(1)))(ctx.getText)
  override def visitDigitIdentifier(ctx: DigitIdentifierContext): BaseExpression =
    Identifier(ctx.getText, None, None)(ctx.getText)

  override def visitQualifiedName(ctx: QualifiedNameContext): Identifier = {
    val (identifiers, quote) = unwrapQualifiedName(ctx.identifier())
    val qualifiers = identifiers.dropRight(1)
    Identifier(identifiers.last, if (qualifiers.nonEmpty) Some(qualifiers) else None, quote)(ctx.getText)
  }

  // UNARY EXPRESSIONS
  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): BaseExpression =
    UnaryOperator(visit(ctx.valueExpression()), ctx.operator.getText)(getRaw(ctx))
  override def visitLogicalNot(ctx: LogicalNotContext): BaseExpression =
    LogicalNot(visit(ctx.booleanExpression()))(getRaw(ctx))

  // BINARY EXPRESSIONS
  override def visitComparison(ctx: ComparisonContext): BaseExpression =
    BinaryOperator(visit(ctx.left), visit(ctx.right), ctx.comparisonOperator().getText)(getRaw(ctx))
  override def visitLogicalBinary(ctx: LogicalBinaryContext): BaseExpression =
    BinaryOperator(visit(ctx.left), visit(ctx.right), ctx.operator.getText)(getRaw(ctx))
  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): BaseExpression =
    BinaryOperator(visit(ctx.left), visit(ctx.right), ctx.operator.getText)(getRaw(ctx))
  override def visitConcatenation(ctx: ConcatenationContext): BaseExpression =
    BinaryOperator(visit(ctx.left), visit(ctx.right), ctx.CONCAT().getText)(getRaw(ctx))
  override def visitIsPredicated(ctx: IsPredicatedContext): BaseExpression =
    BinaryOperator(visit(ctx.left), visit(ctx.isPredicate), "IS")(getRaw(ctx))

  // SUB QUERY EXPRESSIONS
  override def visitExists(ctx: ExistsContext): BaseExpression = Exists(SubQuery(getText(ctx.query())))(getRaw(ctx))
  override def visitSubqueryExpression(ctx: SubqueryExpressionContext): BaseExpression = SubQuery(getText(ctx.query()))

  // PROJECTION EXPRESSIONS
  override def visitSelectSingle(ctx: SelectSingleContext): BaseExpression = {
    val expr = visit(ctx.expression())
    Option(ctx.identifier()).map(visit).collect{
      case i: Identifier => Alias(expr, i)(getRaw(ctx))
    }.getOrElse(expr)
  }

  override def visitSelectAll(ctx: SelectAllContext): BaseExpression = {
    val qualified = Option(ctx.qualifiedName()).map(e => unwrapQualifiedName(e.identifier()))
    Identifier("*", qualified.map(_._1), qualified.flatMap(_._2))(getText(ctx))
  }

  // GENERAL EXPRESSIONS
  //  override def visitDistinctFrom(ctx: DistinctFromContext): SqlBaseExpression = super.visitDistinctFrom(ctx)
  override def visitBooleanPredicate(ctx: BooleanPredicateContext): BaseExpression =
    injectNot(ctx.NOT(), Literal(ctx.value.getText, None)(ctx.value.getText))
  override def visitNullPredicate(ctx: NullPredicateContext): BaseExpression =
    injectNot(ctx.NOT(), getLiteral(ctx.NULL()))
  override def visitInSubquery(ctx: InSubqueryContext): BaseExpression =
    In(visit(ctx.left), Option(ctx.NOT()).nonEmpty, None, Some(SubQuery(getText(ctx.query()))))(getRaw(ctx))
  override def visitInList(ctx: InListContext): BaseExpression =
    In(visit(ctx.left), Option(ctx.NOT()).nonEmpty, Some(ctx.expression().asScala.toList.map(visit)), None)(getRaw(ctx))
  override def visitBetween(ctx: BetweenContext): BaseExpression =
    Between(visit(ctx.left), visit(ctx.lower), visit(ctx.upper), Option(ctx.NOT()).nonEmpty)(getRaw(ctx))
  override def visitLike(ctx: LikeContext): BaseExpression =
    Like(visit(ctx.left), visit(ctx.pattern), Option(ctx.NOT()).nonEmpty, Option(ctx.escape).map(visit))(getRaw(ctx))
  override def visitCast(ctx: CastContext): BaseExpression =
    Cast(visit(ctx.expression()), getText(ctx.`type`()))(getRaw(ctx))

  override def visitFunctionCall(ctx: FunctionCallContext): BaseExpression = {
    val name = visitQualifiedName(ctx.qualifiedName()).text
    SqlFunction(name, Option(ctx.expression()).map(_.asScala.map(visit).toList))(getRaw(ctx))
  }

  override def visitSimpleCase(ctx: SimpleCaseContext): BaseExpression =
    Case(ctx.whenClause().asScala.map{ when =>
      When(visit(when.condition), visit(when.result))(getRaw(when))
    }.toList, Some(visit(ctx.valueExpression())), Option(ctx.elseExpression).map(visit))(getRaw(ctx))

  override def visitSearchedCase(ctx: SearchedCaseContext): BaseExpression =
    Case(ctx.whenClause().asScala.map { when =>
      When(visit(when.condition), visit(when.result))(getRaw(when))
    }.toList, None, Option(ctx.elseExpression).map(visit))(getRaw(ctx))

  override def visitIf(ctx: IfContext): BaseExpression =
    Case(List(
      When(visit(ctx.ifClause), visit(ctx.thenClause))(() => s"IF (${getText(ctx.ifClause)}) ${getText(ctx.thenClause)}")
    ), None, Some(visit(ctx.elseClause)))(getRaw(ctx))

  override def visitRawLiteral(ctx: RawLiteralContext): BaseExpression =
    Raw(getText(ctx.sqlString()).stripSuffix("'").stripPrefix("'"))

  override def shouldVisitNextChild(node: RuleNode, currentResult: BaseExpression): Boolean = Option(currentResult).isEmpty

  private def getText(ctx: RuleContext): String = tokenStream.getText(ctx)
  private def getRaw(ctx: RuleContext): () => String = () => getText(ctx)

  private def getLiteral(ctx: ParseTree): Literal = {
    val text = ctx.getText
    Literal(text, None)(text)
  }

  private def injectNot(not: TerminalNode, expr: BaseExpression): BaseExpression = {
    Option(not).map { n =>
      LogicalNot(expr)(() => n.getText + " "  + expr.raw)
    }.getOrElse(expr)
  }

  private def unwrapQualifiedName(expressions: java.util.List[IdentifierContext]): (List[String], Option[String]) = {
    val identifiers = expressions.asScala.map(visit).collect{ case i: Identifier => i}.toList
    val quote = identifiers.foldLeft(None: Option[String])((q, i) => q orElse i.quote)
    (identifiers.map(_.value), quote)
  }
}
