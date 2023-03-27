package com.intertrust.parsers

import zio.stream.ZStream
import zio.{Scope, Task, ZIO}

import scala.io.Source

abstract class EventStream[T] {
  protected def parseEvent(columns: Array[String]): Task[T]

  def fromSource(source: Source): ZStream[Scope, Throwable, T] = {
    ZStream.fromZIO(ZIO.fromAutoCloseable(ZIO.succeed(source)))
      .flatMap(source => ZStream.fromIterator(source.getLines()))
      .drop(1) // Drop header
      .mapZIO(asEvent)
  }

  private def asEvent(line: String): Task[T] =
    parseEvent(columns = line.split(','))
      .mapError(e => EventParserException(s"Failed to parse event from '$line'", e))
}

case class EventParserException(message: String, cause: Throwable) extends RuntimeException(message, cause)
