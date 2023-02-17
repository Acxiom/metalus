package com.acxiom.metalus.sql.parser

import org.antlr.v4.runtime.{CharStream, CharStreams, IntStream}
import org.antlr.v4.runtime.misc.Interval

object UpperCaseCharStream {
  def fromString(text: String): UpperCaseCharStream = new UpperCaseCharStream(CharStreams.fromString(text))
}

class UpperCaseCharStream(wrapped: CharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName: String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  //noinspection ScalaStyle
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}
