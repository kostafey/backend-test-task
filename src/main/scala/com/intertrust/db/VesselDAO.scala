package com.intertrust.db

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import com.intertrust.protocol.Movement

case class Vessel(
  id: String,
  var updated: Instant,
  var techniciansInside: Seq[Technician]
)

object VesselDAO {
  private val vesselesList = new ArrayBuffer[Vessel]()

  def get(id: String): Option[Vessel] = {
    vesselesList.find(_.id == id)
  }

  def update(vessel: Vessel,
             updated: Instant,
             technician: Technician,
             movement: Movement): Vessel = {
    vessel.updated = updated
    vessel.techniciansInside = movement match {
      case Movement.Enter =>  vessel.techniciansInside :+ technician
      case Movement.Exit => vessel.techniciansInside.filter(_.id == technician.id)
    }
    vessel
  }

  def add(id: String,
          updated: Instant,
          technician: Technician,
          movement: Movement): Vessel = {
    val vessel = Vessel(
      id = id,
      updated = updated,
      techniciansInside = movement match {
        case Movement.Enter => Vector(technician)
        case Movement.Exit => Seq.empty
      }
    )
    vesselesList += vessel
    vessel
  }
}
