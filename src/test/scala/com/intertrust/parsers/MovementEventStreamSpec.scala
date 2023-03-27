package com.intertrust.parsers

import com.intertrust.protocol.Movement.{Enter, Exit}
import com.intertrust.protocol._
import zio.stream.ZSink
import zio.test.Assertion.failsWithA
import zio.test.{Spec, ZIOSpecDefault, assert, assertTrue}
import zio.{Chunk, Scope}

import java.time.Instant
import scala.io.Source

object MovementEventStreamSpec extends ZIOSpecDefault {
  def spec: Spec[Scope, Throwable] =
    suite("Movement event stream")(
      suite("successfully parse movement events")(
        test("single entry to vessel") {
          verifyEventParsing(
            input =
              """Date,Location,Person,Movement type
                |23.11.2015 06:37,Vessel 235098384,P1,Enter""".stripMargin,
            expectedEvents = Chunk(MovementEvent("P1", Vessel("235098384"), Enter, Instant.parse("2015-11-23T06:37:00.00Z")))
          )
        },
        test("single exit from turbine") {
          verifyEventParsing(
            input =
              """Date,Location,Person,Movement type
                |24.11.2015 10:57,D5A,P40,Exit""".stripMargin,
            expectedEvents = Chunk(MovementEvent("P40", Turbine("D5A"), Exit, Instant.parse("2015-11-24T10:57:00.00Z")))
          )
        },
        test("multiple events") {
          verifyEventParsing(
            input =
              """Date,Location,Person,Movement type
                |23.11.2015 09:21,E8J,P57,Exit
                |23.11.2015 09:21,Vessel 235090838,P57,Enter
                |23.11.2015 09:26,Vessel 235090838,P19,Exit
                |23.11.2015 09:26,F3P,P19,Enter""".stripMargin,
            expectedEvents = Chunk(
              MovementEvent("P57", Turbine("E8J"), Exit, Instant.parse("2015-11-23T09:21:00.00Z")),
              MovementEvent("P57", Vessel("235090838"), Enter, Instant.parse("2015-11-23T09:21:00.00Z")),
              MovementEvent("P19", Vessel("235090838"), Exit, Instant.parse("2015-11-23T09:26:00.00Z")),
              MovementEvent("P19", Turbine("F3P"), Enter, Instant.parse("2015-11-23T09:26:00.00Z"))
            )
          )
        },
      ),
      suite("fail to parse movement events")(
        test("malformed row") {
          val input =
            """Date,Location,Person,Movement type
              |bunch of monkeys""".stripMargin

          for {
            result <- eventsFrom(input).exit
          } yield assert(result)(failsWithA[EventParserException])
        },
        test("invalid movement type") {
          val input =
            """Date,Location,Person,Movement type
              |24.11.2015 10:57,D5A,P40,Leave""".stripMargin

          for {
            result <- eventsFrom(input).exit
          } yield assert(result)(failsWithA[EventParserException])
        },
        test("invalid movement date format") {
          val input =
            """Date,Location,Person,Movement type
              |23-11-2015 09:21:00,E8J,P57,Enter""".stripMargin

          for {
            result <- eventsFrom(input).exit
          } yield assert(result)(failsWithA[EventParserException])
        },
      )
    )

  private def verifyEventParsing(input: String, expectedEvents: Chunk[MovementEvent]) = for {
    events <- eventsFrom(input)
  } yield assertTrue(events == expectedEvents)

  private def eventsFrom(input: String) =
    MovementEventStream
      .fromSource(Source.fromString(input))
      .run(ZSink.collectAll)
}
