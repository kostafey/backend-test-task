package com.intertrust

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date

import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault, Schedule}
import zio.durationInt
import zio.stream.{ZStream, ZPipeline}
import zio.Chunk

import com.intertrust.processing.AlertsSink
import com.intertrust.parsers.{MovementEventStream, TurbineEventStream}
import com.intertrust.protocol.{TurbineAlert, MovementAlert, TurbineStatus,
  Event, MovementEvent, TurbineEvent, Alert, Vessel, Turbine, Movement}

object Simulator extends ZIOAppDefault {
  val turbinesFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"))

  val simulationStart = new Date()
  val theBeginningOfTime: Instant = Instant.from(turbinesFmt.parse("2015-11-23 00:00:00"))
  val speed: Int = 6000

  def diffInMilliSeconds(d1: Date, d2: Date): Int = {
    (Math.abs(d1.getTime() - d2.getTime())).toInt
  }

  def getRelativeTime(): Instant = {
    val millis: Int = diffInMilliSeconds(simulationStart, new Date()) * speed
    theBeginningOfTime.plusMillis(millis)
  }

  def getPause(t: Instant): Int = {
    val p = t.minusMillis(getRelativeTime().toEpochMilli()).toEpochMilli() / speed
    if (p > 0) {
      p.intValue()
    } else {
      0
    }
  }

  val hours4 = 4*60*60
  val minutes3 = 3*60

  def getTechnician(m: MovementEvent): db.Technician = {
    db.TechnicianDAO.get(m.engineerId) match {
      case Some(existingTechnician) => {
        existingTechnician
      }
      case None => {
        db.TechnicianDAO.add(
          id = m.engineerId,
          updated = m.timestamp,
          inVessel = None,
          inTurbine = None)
      }
    }
  }

  def technicianHandler(m: MovementEvent,
                        v: Vessel,
                        alerts: ArrayBuffer[Alert]): db.Technician = {
    val technician = getTechnician(m)
    val inVessel: Option[String] = m.movement match {
      case Movement.Enter => {
        if (technician.inTurbine.isDefined) {
          alerts += MovementAlert(
            timestamp = m.timestamp,
            engineerId = m.engineerId,
            error = "Technician moves onto a ship without " +
              "having exited a turbine")
        }
        Some(v.id)
      }
      case Movement.Exit => {
        if (!technician.inVessel.isDefined) {
          alerts += MovementAlert(
            timestamp = m.timestamp,
            engineerId = m.engineerId,
            error = "Technician exits a ship without " +
              "having entered a ship")
        }
        None
      }
    }
    db.TechnicianDAO.update(
      technician = technician,
      updated = m.timestamp,
      inVessel = inVessel,
      inTurbine = None // clean up state
    )
    technician
  }

  def technicianHandler(m: MovementEvent,
                        t: Turbine,
                        alerts: ArrayBuffer[Alert]): db.Technician = {
    val technician = getTechnician(m)
    val inTurbine: Option[String] = m.movement match {
      case Movement.Enter => {
        if (technician.inVessel.isDefined) {
          alerts += MovementAlert(
            timestamp = m.timestamp,
            engineerId = m.engineerId,
            error = "Technician moves onto a turbine without " +
              "having exited a ship")
        }
        Some(t.id)
      }
      case Movement.Exit => {
        if (!technician.inTurbine.isDefined) {
          alerts += MovementAlert(
            timestamp = m.timestamp,
            engineerId = m.engineerId,
            error = "Technician exits a turbine without " +
              "having entered a turbine")
        }
        None
      }
    }
    db.TechnicianDAO.update(
      technician = technician,
      updated = m.timestamp,
      inVessel = None, // clean up state
      inTurbine = inTurbine)
    technician
  }

  def eventHandler(event: Event): Chunk[Alert] = {
    val alerts = new ArrayBuffer[Alert]()

    event match {
      case m: MovementEvent => {
        m.location match {
          case v: Vessel => {
            db.VesselDAO.get(v.id) match {
              case Some(existingVessel) => {
                db.VesselDAO.update(
                  vessel = existingVessel,
                  updated = m.timestamp,
                  technician = technicianHandler(m, v, alerts),
                  movement = m.movement)
              }
              case None => {
                db.VesselDAO.add(
                  id = v.id,
                  updated = m.timestamp,
                  technician = technicianHandler(m, v, alerts),
                  movement = m.movement)
              }
            }
          }
          case t: Turbine => {
            db.TurbineDAO.get(t.id) match {
              case Some(existingTurbine) => {
                db.TurbineDAO.update(
                  turbine = existingTurbine,
                  updated = m.timestamp,
                  technician = technicianHandler(m, t, alerts),
                  movement = m.movement)
              }
              case None => {
                // Movement inside unseen earlier turbine is a bit unexpected.
                val newTurbine = db.TurbineDAO.add(
                  id = t.id,
                  updated = m.timestamp,
                  updatedStatus = m.timestamp,
                  generation = 0.0,
                  status = TurbineStatus.Broken)
                db.TurbineDAO.update(
                  turbine = newTurbine,
                  updated = m.timestamp,
                  technician = technicianHandler(m, t, alerts),
                  movement = m.movement)
              }
            }
          }
        }
      }
      case te: TurbineEvent => {
        db.TurbineDAO.get(te.turbineId) match {
          case Some(existingTurbine) => {
            if (existingTurbine.status == TurbineStatus.Working &&
                  te.status == TurbineStatus.Broken) {
              // turbine stops working
              alerts += TurbineAlert(
                timestamp = te.timestamp,
                turbineId = te.turbineId,
                error = "Turbine stops working")
            }
            if (existingTurbine.status == TurbineStatus.Broken &&
                  existingTurbine.updatedStatus.isBefore(
                    te.timestamp.minusSeconds(hours4)) &&
                  existingTurbine.techniciansInside.isEmpty) {
              // turbine has been in a `Broken` state for more than 4 hours
              // and no technician has entered the turbine yet to fix it
              alerts += TurbineAlert(
                timestamp = te.timestamp,
                turbineId = te.turbineId,
                error = "Turbine no repair")
            }
            if (existingTurbine.technicianExited.isDefined &&
                  existingTurbine.technicianExited.get.isBefore(
                    Instant.now().minusSeconds(minutes3)) &&
                  te.status == TurbineStatus.Broken) {
              // technician exits a turbine without having repaired the turbine;
              // the condition here is that if the turbine continues
              // in a `Broken` state for more than 3 minutes after a technician
              // has exited the turbine
              alerts += TurbineAlert(
                timestamp = te.timestamp,
                turbineId = te.turbineId,
                error = "Turbine not fixed by technician")
            }
            db.TurbineDAO.update(
              turbine = existingTurbine,
              updated = te.timestamp,
              updatedStatus = if ((existingTurbine.status != te.status) ||
                                    // Notify for repear every 4 hours - reset time.
                                    (existingTurbine.status == TurbineStatus.Broken &&
                                       existingTurbine.updatedStatus.isBefore(
                                         te.timestamp.minusSeconds(hours4)) &&
                                       existingTurbine.techniciansInside.isEmpty)) {
                te.timestamp
              } else {
                existingTurbine.updatedStatus
              },
              generation = te.generation,
              status = te.status)
          }
          case None => db.TurbineDAO.add(
            id = te.turbineId,
            updated = te.timestamp,
            updatedStatus = te.timestamp,
            generation = te.generation,
            status = te.status)
        }
      }
    }
    Chunk.fromIterable(alerts)
  }

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] = {
    val movementEvents = MovementEventStream.fromSource(Source.fromURL(getClass.getResource("/movements.csv")))
    val turbineEvents = TurbineEventStream.fromSource(Source.fromURL(getClass.getResource("/turbines.csv")))
    val alertsSink = AlertsSink.console

    val movementEventsByTime = movementEvents.map(m =>
      ZStream.succeed(m).schedule(Schedule.spaced(getPause(m.timestamp).millisecond)))
      .flatten

    val turbineEventsByTime = turbineEvents.map(m =>
      ZStream.succeed(m).schedule(Schedule.spaced(getPause(m.timestamp).millisecond)))
      .flatten

    // Implement events processing pipeline that sends alerts to the `alertsSink`
    val eventPipeline = ZPipeline.map(eventHandler).flattenChunks

    for {
      movementFiber <- movementEventsByTime.via(eventPipeline).run(alertsSink).fork
      turbineFiber <- turbineEventsByTime.via(eventPipeline).run(alertsSink).fork
      _ <- movementFiber.join
      _ <- turbineFiber.join
    } yield ()
  }
}
