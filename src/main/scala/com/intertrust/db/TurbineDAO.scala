package com.intertrust.db

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import com.intertrust.protocol.TurbineStatus
import com.intertrust.protocol.Location
import com.intertrust.protocol.Movement

case class Turbine(
  id: String,
  var status: TurbineStatus,
  var updatedStatus: Instant,
  var updated: Instant,
  var generation: Double,
  var technicianExited: Option[Instant] = None,
  var techniciansInside: Seq[Technician] = Seq.empty
)

object TurbineDAO {
  private val turbinesList = new ArrayBuffer[Turbine]()

  def get(id: String): Option[Turbine] = {
    turbinesList.find(_.id == id)
  }

  def update(turbine: Turbine,
             updatedStatus: Instant,
             updated: Instant,
             generation: Double,
             status: TurbineStatus): Turbine = {
    turbine.updated = updated
    turbine.updatedStatus = updatedStatus
    turbine.generation = generation
    turbine.status = status
    turbine
  }

  def update(turbine: Turbine,
             updated: Instant,
             technician: Technician,
             movement: Movement): Turbine = {
    turbine.updated = updated
    turbine.techniciansInside = movement match {
      case Movement.Enter =>
        turbine.technicianExited = None
        turbine.techniciansInside :+ technician
      case Movement.Exit => {
        turbine.technicianExited = Some(updated)
        turbine.techniciansInside.filter(_.id == technician.id)
      }
    }
    turbine
  }

  def add(id: String,
          updated: Instant,
          updatedStatus: Instant,
          generation: Double,
          status: TurbineStatus): Turbine = {
    val turbine = Turbine(
      id = id,
      status = status,
      updated = updated,
      updatedStatus = updatedStatus,
      generation = generation)
    turbinesList += turbine
    turbine
  }
}
