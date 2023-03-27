package com.intertrust.protocol

import java.time.Instant

sealed trait Alert

case class TurbineAlert(timestamp: Instant, turbineId: String, error: String) extends Alert

case class MovementAlert(timestamp: Instant, engineerId: String, error: String) extends Alert
