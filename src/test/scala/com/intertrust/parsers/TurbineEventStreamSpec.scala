package com.intertrust.parsers

import com.intertrust.protocol.TurbineStatus.{Broken, Working}
import com.intertrust.protocol._
import zio.{Chunk, Scope}
import zio.stream.ZSink
import zio.test.Assertion.failsWithA
import zio.test.{Spec, ZIOSpecDefault, assert, assertTrue}

import java.time.Instant
import scala.io.Source

object TurbineEventStreamSpec extends ZIOSpecDefault {
  def spec: Spec[Scope, Throwable] =
    suite("Turbine event stream")(
      suite("successfully parse turbine events")(
        test("single working event") {
          verifyEventParsing(
            input =
              """Date,ID,ActivePower (MW),Status
                |2015-11-23 00:00:00,B4B,3.18,Working""".stripMargin,

            expectedEvents = Chunk(TurbineEvent("B4B", Working, 3.18, Instant.parse("2015-11-23T00:00:00.00Z")))
          )
        },
        test("single broken event") {
          verifyEventParsing(
            input =
              """Date,ID,ActivePower (MW),Status
                |2015-11-24 03:12:00,B3A,-0.11,Broken""".stripMargin,
            expectedEvents = Chunk(TurbineEvent("B3A", Broken, -0.11, Instant.parse("2015-11-24T03:12:00.00Z")))
          )
        },
        test("multiple events") {
          verifyEventParsing(
            input =
              """Date,ID,ActivePower (MW),Status
                |2015-11-23 03:33:00,H7R,3.00,Working
                |2015-11-23 03:33:00,B3A,-0.11,Broken
                |2015-11-23 03:33:00,E7J,3.32,Working
                |2015-11-23 03:33:00,F10H_PR,3.09,Working""".stripMargin,
            expectedEvents = Chunk(
              TurbineEvent("H7R", Working, 3.00, Instant.parse("2015-11-23T03:33:00Z")),
              TurbineEvent("B3A", Broken, -0.11, Instant.parse("2015-11-23T03:33:00Z")),
              TurbineEvent("E7J", Working, 3.32, Instant.parse("2015-11-23T03:33:00Z")),
              TurbineEvent("F10H_PR", Working, 3.09, Instant.parse("2015-11-23T03:33:00Z"))
            )
          )
        }
      ),
      suite("fail to parse turbine events")(
        test("malformed row") {
          val input =
            """Date,ID,ActivePower (MW),Status
              |bunch of monkeys""".stripMargin

          for {
            result <- eventsFrom(input).exit
          } yield assert(result)(failsWithA[EventParserException])
        },
        test("invalid status") {
          val input =
            """Date,ID,ActivePower (MW),Status
              |2015-11-24 03:12:00,B3A,-0.11,Failed""".stripMargin

          for {
            result <- eventsFrom(input).exit
          } yield assert(result)(failsWithA[EventParserException])
        },
        test("invalid date format") {
          val input =
            """Date,ID,ActivePower (MW),Status
              |24.11.2015 03:12,B3A,-0.11,Broken""".stripMargin

          for {
            result <- eventsFrom(input).exit
          } yield assert(result)(failsWithA[EventParserException])
        },
      )
    )

  private def verifyEventParsing(input: String, expectedEvents: Chunk[TurbineEvent]) = for {
    events <- eventsFrom(input)
  } yield assertTrue(events == expectedEvents)

  private def eventsFrom(input: String) =
    TurbineEventStream
      .fromSource(Source.fromString(input))
      .run(ZSink.collectAll)
}
