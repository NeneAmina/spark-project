import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import java.time.OffsetDateTime

object GazStationParser {
  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false)

  // Method to parse JSON string into GasStation object
  def fromJson(jsonString: String): Option[GasStation] = {
    try {
      Some(objectMapper.readValue(jsonString, classOf[GasStation]))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }
}

case class GasStation(
                       adresse: String,
                       com_arm_code: String,
                       com_arm_name: String,
                       cp: String,
                       dep_code: String,
                       dep_name: String,
                       epci_code: String,
                       epci_name: String,
                       geom: Geom,
                       horaires: String,
                       horaires_automate_24_24: String,
                       id: String,
                       pop: String,
                       prix_id: String,
                       prix_maj: OffsetDateTime,
                       prix_nom: String,
                       prix_valeur: Double,
                       reg_code: String,
                       reg_name: String,
                       rupture: Option[String],
                       rupture_debut: Option[OffsetDateTime],
                       rupture_nom: Option[String],
                       services_service: List[String],
                       ville: String
                     )

case class Geom(lat: Double, lon: Double)
