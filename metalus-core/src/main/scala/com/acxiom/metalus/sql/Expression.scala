package com.acxiom.metalus.sql

import com.acxiom.metalus.sql.parser._

sealed trait BaseExpression extends Product {

  private lazy val containsChild = children.toSet
  def children: List[BaseExpression]
  def text: String = raw
  def raw: String

  def walk[T](initial: T)(enter: PartialFunction[(T, BaseExpression), T] = PartialFunction.empty,
                          exit: PartialFunction[(T, BaseExpression), T] = PartialFunction.empty): T = {
    val default: PartialFunction[(T, BaseExpression), T] = {
      case (agg, _) => agg
    }
    val onEnter = enter orElse default
    val onExit = exit orElse default

    def traverse(init: T, node: BaseExpression): T = {
      val enterRes = onEnter((init, node))
      val childrenRes = node.children.foldLeft(enterRes)(traverse)
      onExit((childrenRes, node))
    }

    traverse(initial, this)
  }

  def find(func: BaseExpression => Boolean): Option[BaseExpression] = if (func(this)){
    Some(this)
  } else {
    children.foldLeft(None: Option[BaseExpression])((f, c) => f orElse c.find(func))
  }

  def exists(func: BaseExpression => Boolean): Boolean = find(func).isDefined

  /**
   * Run a function down the tree and aggregate the result. Equivalent to walk without an exit function
   * @param initial starting value.
   * @param func    function to run down the tree.
   * @return aggregation result of running func down the tree.
   */
  def foldLeft[T](initial: T)(func: PartialFunction[(T, BaseExpression), T]): T = walk(initial)(func)



  def transform(pf: PartialFunction[BaseExpression, BaseExpression]) : BaseExpression = transformDown(pf)

  def transformDown(pf: PartialFunction[BaseExpression, BaseExpression]): BaseExpression =
    pf.applyOrElse(this, identity[BaseExpression]).mapChildren(_.transformDown(pf))

  def transformUp(pf: PartialFunction[BaseExpression, BaseExpression]): BaseExpression =
    pf.applyOrElse(this.mapChildren(_.transformUp(pf)), identity[BaseExpression])

  // copied from spark's TreeNode trait, faster than calling productIterator.toArray
  protected def mapProductIterator(f: Any => AnyRef): Array[AnyRef] = {
    val arr = Array.ofDim[AnyRef](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

  //noinspection ScalaStyle
  def mapChildren(f: BaseExpression => BaseExpression): BaseExpression = if (children.isEmpty) {
    this
  } else {
    var changed = false
    def getNewChild(child: BaseExpression): BaseExpression = {
      val newChild = f(child)
      if (child.eq(newChild) || child == newChild) {
        child
      } else {
        changed = true
        newChild
      }
    }
    val args = mapProductIterator {
      case child: BaseExpression if containsChild(child) => getNewChild(child)
      case Some(child: BaseExpression) if containsChild(child) => Some(getNewChild(child))
      case m: Map[_, _] => m.mapValues {
        case child: BaseExpression if containsChild(child) =>getNewChild(child)
        case Some(child: BaseExpression) if containsChild(child) => getNewChild(child)
        case other => other
      }.view.force.toMap
      case Some(m: Map[_, _]) => Some(m.mapValues {
        case child: BaseExpression if containsChild(child) => getNewChild(child)
        case Some(child: BaseExpression) if containsChild(child) => Some(getNewChild(child))
        case other => other
      }.view.force.toMap)
      case i: Iterable[_] => i.map{
        case child: BaseExpression if containsChild(child) => getNewChild(child)
        case Some(child: BaseExpression) if containsChild(child) => Some(getNewChild(child))
        case other => other
      }
      case Some(i: Iterable[_]) => Some(i.map {
        case child: BaseExpression if containsChild(child) => getNewChild(child)
        case Some(child: BaseExpression) if containsChild(child) => Some(getNewChild(child))
        case other => other
      })
      case nonChild: AnyRef => nonChild
      case null => null
    }

    if (changed) makeCopy(args) else this
  }

  // Based on code from spark's TreeNode, with a lot of checks ripped out and assumptions
  // that can only be made because BaseExpression is sealed
  protected def makeCopy(newArgs: Array[AnyRef]): BaseExpression = {
    val allCtors = getClass.getConstructors
    if (newArgs.isEmpty && allCtors.isEmpty) {
      // This is a singleton object which doesn't have any constructor. Just return `this` as we can't copy it.
      this
    } else {
      // Skip no-arg constructors that are just there for kryo.
      val ctors = allCtors.filter(_.getParameterTypes.length != 0)
      if (ctors.isEmpty) {
        sys.error(s"No valid constructor for ${getClass.getSimpleName}")
      }
      val defaultCtor = ctors.maxBy(_.getParameterTypes.length)

      val allArgs = this match {
        case _: LeafBaseExpression => newArgs :+ this.raw
        case ie: InternalExpression => newArgs :+ ie.getRaw
      }
      defaultCtor.newInstance(allArgs: _*).asInstanceOf[BaseExpression]
    }
  }
}

trait InternalExpression extends BaseExpression {
  override def raw: String = getRaw()

  def getRaw: () => String
}

abstract class LeafBaseExpression(override val raw: String) extends BaseExpression {
  override def children: List[BaseExpression] = Nil
}

trait UnaryExpression extends InternalExpression {
  def expr: BaseExpression
  def operator: String

  override def children: List[BaseExpression] = List(expr)
  override def text: String = s"$operator$expr"
}

trait BinaryExpression extends InternalExpression {
  def left: BaseExpression
  def right: BaseExpression
  def operator: String

  override def children: List[BaseExpression] = List(left, right)

  override def text: String = s"${left.text} $operator ${right.text}"
}

final case class BinaryOperator(left: BaseExpression, right: BaseExpression, operator: String)
                               (implicit val getRaw: () => String) extends BinaryExpression

final case class UnaryOperator(expr: BaseExpression, operator: String)
                              (implicit val getRaw: () => String) extends UnaryExpression

final case class LogicalNot(expr: BaseExpression)
                           (implicit val getRaw: () => String) extends UnaryExpression {
  override def operator: String = "NOT"
  override def text: String = s"$operator $expr"
}

final case class Alias(expr: BaseExpression, alias: Identifier)
                      (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = List(expr, alias)
  override def text: String = s"${expr.text} AS ${alias.text}"
}

final case class Assignment(column: Identifier, expr: BaseExpression)
                           (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = List(column, expr)
  override def text: String = s"${column.text} = ${expr.text}"
}

final case class Like(left: BaseExpression, expr: BaseExpression, not: Boolean, escape: Option[BaseExpression])
                     (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = expr +: escape.toList
  override def text: String =
    s"${left.text} ${if(not) "NOT " else ""}LIKE ${expr.text}${escape.map(e => s"ESCAPE ${e.text}").mkString}"
}

final case class Between(expr: BaseExpression, lower: BaseExpression, upper: BaseExpression, not: Boolean)
                        (implicit val getRaw: () => String) extends InternalExpression {
  override def text: String = s"${expr.text} ${if(not) "NOT " else ""}BETWEEN ${lower.text} AND ${upper.text}"
  override def children: List[BaseExpression] = List(expr, lower, upper)
}

final case class In(left: BaseExpression, not: Boolean, expressions: Option[List[BaseExpression]], query: Option[SubQuery])
                   (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = left +: expressions.getOrElse(Nil)
  override def text: String = {
    val in = expressions.map{ exprs =>
      exprs.map(_.text).mkString(", ")
    } orElse query.map(_.text)
    s"${left.text} ${if(not) "NOT " else ""} IN ($in)"
  }
}

final case class Cast(expr: BaseExpression, dataType: String)
                     (implicit val getRaw: () => String) extends InternalExpression {
  override def text: String = s"CAST(${expr.text} AS ${dataType})"

  override def children: List[BaseExpression] = List(expr)
}

final case class SqlFunction(name: String, parameters: Option[List[BaseExpression]])
                            (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = parameters.toList.flatten
  override def text: String = s"$name(${children.map(_.text).mkString(", ")})"
}

final case class Case(whenClauses: List[When], expression: Option[BaseExpression], elseClause: Option[BaseExpression])
                     (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = expression.toList ++ whenClauses ++ elseClause

  override def text: String =
    s"CASE${expression.map(" " + _.text).mkString} ${whenClauses.map(_.text).mkString("\n")}${elseClause.map(" " + _.text).mkString}"
}

final case class When(left: BaseExpression, right: BaseExpression)
                     (implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = List(left, right)

  override def text: String = s"WHEN ${left.text} THEN ${right.text}"
}

final case class Identifier(value: String, qualifiers: Option[List[String]], quote: Option[String])(raw: String)
  extends LeafBaseExpression(raw) {
  override def text: String = quote.map { q =>
    val quotedValue = if (value == "*") value else s"$q$value$q"
    (qualifiers.map(_.map(qualifier => s"$q$qualifier$q")).getOrElse(List()) :+ quotedValue).mkString(".")
  } getOrElse (qualifiers.getOrElse(List()) :+ value).mkString(".")

  def qualifiedName: String = (qualifiers.getOrElse(List()) :+ value).mkString(".")
}

final case class Literal(value: String, quote: Option[String])(raw: String) extends LeafBaseExpression(raw) {
  override def text: String = quote.map(q => s"$q$value$q").getOrElse(value)
}

final case class Interval(value: String, unit: String, sign: Option[String], toUnit: Option[String])(raw: String)
  extends LeafBaseExpression(raw)

// act as a pass-through for sub-queries
final case class Exists(subQuery: SubQuery)(implicit val getRaw: () => String) extends InternalExpression {
  override def children: List[BaseExpression] = List(subQuery)
}
final case class SubQuery(override val raw: String) extends LeafBaseExpression(raw)
final case class Raw(override val raw: String) extends LeafBaseExpression(raw)

object NullLiteral {
  def unapply(expr: BaseExpression): Option[Literal] = expr match {
    case nl @ Literal(n, _) if n equalsIgnoreCase "null" => Some(nl)
    case _ => None
  }
}

object BooleanLiteral {
  def unapply(expr: BaseExpression): Option[Literal] = expr match {
    case bl @ Literal(b, _) if b.equalsIgnoreCase("true") || b.equalsIgnoreCase("false") =>
      Some(bl)
    case _ => None
  }
}

object NumericLiteral {
  def unapply(expr: BaseExpression): Option[Literal] = expr match {
    case nl @ Literal(n, _) if isNumeric(n) => Some(nl)
    case _ => None
  }

  private def isNumeric(value: String): Boolean = {
    (value.indexOf('.') == value.lastIndexOf('.')) && value.forall{
      case '.' => true
      case c => c.isDigit
    }
  }
}

object DecimalLiteral {
  def unapply(expr: BaseExpression): Option[Literal] = expr match {
    case nl @ Literal(n, _) if isNumeric(n) => Some(nl)
    case _ => None
  }

  private def isNumeric(value: String): Boolean = {
    val index = value.indexOf('.')
    index > -1 && index == value.lastIndexOf('.') && value.forall {
      case '.' => true
      case c => c.isDigit
    }
  }
}

object IntegerLiteral {
  def unapply(expr: BaseExpression): Option[Literal] = expr match {
    case nl @ Literal(n, _) if n.forall(_.isDigit) => Some(nl)
    case _ => None
  }
}

object SelectAll {
  def unapply(expr: BaseExpression): Option[Identifier] = expr match {
    case i @ Identifier("*", _, _) => Some(i)
  }
}

object Expression {
  def apply(text: String): Expression = DefaultExpression(text)

  def expr(text: String): Expression = DefaultExpression(text)

  def col(text: String): Column = {
    val values = text.split('.')
    Column(Identifier(values.last, if (values.length > 1) Some(values.dropRight(1).toList) else None, None)(text))
  }

  def lit(text: String): Column = {
    val quote = text.headOption.collect{
      case q@('`' | '"') => q.toString
    }
    val value = quote.map(q => text.stripPrefix(q).stripSuffix(q)).getOrElse(text)
    Column(Literal(value, quote)(text))
  }
}

trait Expression {

  private lazy val tree: BaseExpression = SqlExpressionParser.parse(text)

  def text: String
  def expressionTree: BaseExpression = tree

  override def toString: String = text

}

case class DefaultExpression(text: String) extends Expression

case class Column(override val expressionTree: BaseExpression) extends Expression {
  override def text: String = expressionTree.text

  private implicit val getRaw: () => String = text _

  def cast(dataType: String): Column = copy(Cast(expressionTree, dataType))

  def as(alias: String): Column = {
    val quote = alias.headOption.collect {
      case q@('`' | '"') => q.toString
    }
    val value = quote.map(q => alias.stripPrefix(q).stripSuffix(q)).getOrElse(alias)
    copy(Alias(expressionTree, Identifier(value, None, quote)(alias)))
  }

}
