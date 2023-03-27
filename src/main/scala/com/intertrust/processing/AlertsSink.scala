package com.intertrust.processing

import com.intertrust.protocol.{Alert, MovementAlert, TurbineAlert}
import zio.ZIO
import zio.stream.{Sink, ZSink}

object AlertsSink {
  val console: Sink[Nothing, Alert, Nothing, Unit] = ZSink.foreach {
    case a: TurbineAlert => ZIO.logError(a.toString)
    case a: MovementAlert => ZIO.logError(a.toString)
  }
}
