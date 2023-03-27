package com.intertrust

import com.intertrust.parsers.{MovementEventStream, TurbineEventStream}
import com.intertrust.processing.AlertsSink
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import scala.io.Source

object Simulator extends ZIOAppDefault {
  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] = {
    val movementEvents = MovementEventStream.fromSource(Source.fromURL(getClass.getResource("movements.cvs")))
    val turbineEvents = TurbineEventStream.fromSource(Source.fromURL(getClass.getResource("movements.cvs")))
    val alertsSink = AlertsSink.console

    // TODO: Implement events processing pipeline that sends alerts to the `alertsSink`
    ???
  }
}
