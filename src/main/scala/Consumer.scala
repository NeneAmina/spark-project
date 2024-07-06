import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.json4s.jackson.JsonMethods.{compact, render}

import java.util.Properties
import scala.jdk.CollectionConverters._
import java.sql.{Connection, DriverManager, PreparedStatement,Timestamp, Types}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.time.OffsetDateTime
object Consumer {
  def main(args: Array[String]): Unit = {

    //nom_du_topic nom_base_de_donnée nom_utilisateur_bd mot_de_pass_bd
    if (args.length < 4) {
      println("Usage: Main <topicName> <dbName> <dbUser> <dbPass>")
      sys.exit(1)
    }

    val topicName = args(0)
    val dbName = args(1)
    val dbUser = args(2)
    val dbPass = args(3)
    val kafkaProps = new Properties()
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](kafkaProps)
    consumer.subscribe(java.util.Collections.singletonList(topicName))

    val dbConnection = getConnection(dbName,dbUser,dbPass)

    try {
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(100))
        for (record <- records.asScala) {
          println(s"Received: ${record.value()}")
          GazStationParser.fromJson(record.value()) match {
            case Some(gasStation) =>
              upsertIntoPostgres(dbConnection, gasStation)
            case None =>
              println("Failed to parse JSON into GasStation")
          }
        }
      }
    } finally {
      consumer.close()
      dbConnection.close()
    }
  }

  def getConnection(dbName:String,userName: String,password:String): Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/${dbName}", userName, password)
  }

  def upsertIntoPostgres(conn: Connection, gasStation: GasStation): Unit = {
    val sql = """
    INSERT INTO gas_stations (
      id, adresse, com_arm_code, com_arm_name, cp, dep_code, dep_name, epci_code, epci_name,
      horaires, horaires_automate_24_24, pop, prix_id, prix_maj, prix_nom, prix_valeur,
      reg_code, reg_name, rupture, rupture_debut, rupture_nom, ville, latitude, longitude
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?
    ) ON CONFLICT (id) DO UPDATE SET
      adresse = EXCLUDED.adresse,
      com_arm_code = EXCLUDED.com_arm_code,
      com_arm_name = EXCLUDED.com_arm_name,
      cp = EXCLUDED.cp,
      dep_code = EXCLUDED.dep_code,
      dep_name = EXCLUDED.dep_name,
      epci_code = EXCLUDED.epci_code,
      epci_name = EXCLUDED.epci_name,
      horaires = EXCLUDED.horaires,
      horaires_automate_24_24 = EXCLUDED.horaires_automate_24_24,
      pop = EXCLUDED.pop,
      prix_id = EXCLUDED.prix_id,
      prix_maj = EXCLUDED.prix_maj,
      prix_nom = EXCLUDED.prix_nom,
      prix_valeur = EXCLUDED.prix_valeur,
      reg_code = EXCLUDED.reg_code,
      reg_name = EXCLUDED.reg_name,
      rupture = EXCLUDED.rupture,
      rupture_debut = EXCLUDED.rupture_debut,
      rupture_nom = EXCLUDED.rupture_nom,
      ville = EXCLUDED.ville,
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude
  """

    val pstmt: PreparedStatement = conn.prepareStatement(sql)

    pstmt.setString(1, gasStation.id)
    pstmt.setString(2, gasStation.adresse)
    pstmt.setString(3, gasStation.com_arm_code)
    pstmt.setString(4, gasStation.com_arm_name)
    pstmt.setString(5, gasStation.cp)
    pstmt.setString(6, gasStation.dep_code)
    pstmt.setString(7, gasStation.dep_name)
    pstmt.setString(8, gasStation.epci_code)
    pstmt.setString(9, gasStation.epci_name)
    pstmt.setString(10, Option(gasStation.horaires).map(h => compact(render(parse(h)))).orNull)
    pstmt.setString(11, gasStation.horaires_automate_24_24)
    pstmt.setString(12, gasStation.pop)
    pstmt.setString(13, gasStation.prix_id)
    Option(gasStation.prix_maj) match {
      case Some(dateTime) => pstmt.setTimestamp(14, Timestamp.from(dateTime.toInstant))
      case None => pstmt.setNull(14, java.sql.Types.TIMESTAMP)
    }
    pstmt.setString(15, gasStation.prix_nom)
    pstmt.setDouble(16, gasStation.prix_valeur)
    pstmt.setString(17, gasStation.reg_code)
    pstmt.setString(18, gasStation.reg_name)
    pstmt.setString(19, gasStation.rupture.map(r => compact(render(parse(r)))).orNull)
    gasStation.rupture_debut match {
      case Some(dateTime) => pstmt.setTimestamp(20, Timestamp.from(dateTime.toInstant))
      case None => pstmt.setNull(20, Types.TIMESTAMP)
    }
    pstmt.setString(21, gasStation.rupture_nom.orNull)
    pstmt.setString(22, gasStation.ville)
    pstmt.setDouble(23, gasStation.geom.lat)
    pstmt.setDouble(24, gasStation.geom.lon)

    pstmt.executeUpdate()
    pstmt.close()
  }

}
