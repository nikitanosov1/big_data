import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class Trip(
                 tripId: Integer,
                 duration: Integer,
                 startDate: LocalDateTime,
                 startStation: String,
                 startTerminal: Integer,
                 endDate: LocalDateTime,
                 endStation: String,
                 endTerminal: Integer,
                 bikeId: Integer,
                 subscriptionType: String,
                 zipCode: String)

object Trip {
  private val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")

  def buildFromRow(row: Array[String]): Trip = {
    Trip(
      tripId = if (row(0).nonEmpty) row(0).toInt else 0,
      duration = if (row(1).nonEmpty) row(1).toInt else 0,
      startDate = if (row(2).nonEmpty) LocalDateTime.parse(row(2), timeFormat) else null,
      startStation = row(3),
      startTerminal = if (row(4).nonEmpty) row(4).toInt else 0,
      endDate = LocalDateTime.parse(row(5), timeFormat),
      endStation = row(6),
      endTerminal = if (row(7).nonEmpty) row(7).toInt else 0,
      bikeId = if (row(8).nonEmpty) row(8).toInt else 0,
      subscriptionType = row(9),
      zipCode = row(10)
    )
  }
}
