package com.intertrust.db

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

case class Technician(
  id: String,
  var updated: Instant,
  var inVessel: Option[String],
  var inTurbine: Option[String]
)

object TechnicianDAO {
  private val techniciansList = new ArrayBuffer[Technician]()

  def get(id: String): Option[Technician] = {
    techniciansList.find(_.id == id)
  }

  def add(
    id: String,
    updated: Instant,
    inVessel: Option[String],
    inTurbine: Option[String]): Technician = {
    val technician = Technician(
      id = id,
      updated = updated,
      inVessel = inVessel,
      inTurbine = inTurbine)
    techniciansList += technician
    technician
  }

  def update(
    technician: Technician,
    updated: Instant,
    inVessel: Option[String],
    inTurbine: Option[String]): Technician = {
    technician.inVessel = inVessel
    technician.inTurbine = inTurbine
    technician
  }
}
