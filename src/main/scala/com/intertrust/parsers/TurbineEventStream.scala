package com.intertrust.parsers

import com.intertrust.protocol.{TurbineEvent, TurbineStatus}
import zio.{Task, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

object TurbineEventStream extends EventStream[TurbineEvent] {
  private val timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"))

  override protected def parseEvent(columns: Array[String]): Task[TurbineEvent] = for {
    turbineId <- ZIO.attempt(columns(1))
    status <- ZIO.attempt(TurbineStatus.withName(columns(3)))
    generation <- ZIO.attempt(columns(2).toDouble)
    timestamp <- ZIO.attempt(Instant.from(timestampFormat.parse(columns(0))))
  } yield TurbineEvent(turbineId, status, generation, timestamp)
}
