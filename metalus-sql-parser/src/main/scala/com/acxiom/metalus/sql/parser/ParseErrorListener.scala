package com.acxiom.metalus.sql.parser

import org.antlr.v4.runtime.{BaseErrorListener, CommonToken, RecognitionException, Recognizer}

case object ParseErrorListener extends BaseErrorListener {
  override def syntaxError(
                            recognizer: Recognizer[_, _],
                            offendingSymbol: scala.Any,
                            line: Int,
                            charPositionInLine: Int,
                            msg: String,
                            e: RecognitionException): Unit = {
    val (l, pos) = offendingSymbol match {
      case token: CommonToken => (line, token.getCharPositionInLine)
      case _ => (line, charPositionInLine)
    }
    throw ParseException(msg, l, pos)
  }
}

case class ParseException(message: String, line: Int, position: Int) extends Exception(message) {
  override def getMessage: String = {
    s"$message\n(line $line, pos $position)\n"
  }
}
