package com.intertrust.parsers

import com.intertrust.protocol._
import zio.{Task, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

object MovementEventStream extends EventStream[MovementEvent] {
  private val timestampFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm").withZone(ZoneId.of("UTC"))
  private val vesselPattern = "Vessel (.*)".r

  override protected def parseEvent(columns: Array[String]): Task[MovementEvent] = for {
    engineerId <- ZIO.attempt(columns(2))
    location <- ZIO.attempt(parseLocationId(columns(1)))
    movement <- ZIO.attempt(Movement.withName(columns(3)))
    timestamp <- ZIO.attempt(Instant.from(timestampFormat.parse(columns(0))))
  } yield MovementEvent(engineerId, location, movement, timestamp)

  private def parseLocationId(s: String): Location = s match {
    case vesselPattern(id) => Vessel(id)
    case other => Turbine(other)
  }
}
