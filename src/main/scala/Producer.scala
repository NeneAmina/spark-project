import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object Producer {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: Main <inputFile> <linesPerSegment>  <intervalSeconds> <topicName>")
      sys.exit(1)
    }

    val inputFile = args(0)
    val linesPerSegment = args(1).toInt
    val intervalSeconds = args(2).toInt
    val topicName= args(3)

    val hadoopHomeDir = "/path/to/hadoop/home" // Set Hadoop home directory
    System.setProperty("hadoop.home.dir", hadoopHomeDir)

    val spark = SparkSession.builder()
      .appName("Spark Kafka Producer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val inputDF = spark.read.json(inputFile)
      println(s"Input file path: $inputFile")
      println(s"Number of records in input DataFrame: ${inputDF.count()}")

      splitJsonFileAndWriteToKafka(inputDF, linesPerSegment, intervalSeconds,topicName)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def splitJsonFileAndWriteToKafka(inputDF: DataFrame, linesPerSegment: Int, intervalSeconds: Int,topicName:String): Unit = {
    val totalRows = inputDF.count()
    println(s"Total number of rows: $totalRows")

    val totalSegments = Math.ceil(totalRows.toDouble / linesPerSegment).toInt
    println(s"Total number of segments: $totalSegments")

    val kafkaProps = new Properties()
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Kafka broker(s)
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    (0 until totalSegments).foreach { segmentIndex =>
      val startRow = segmentIndex * linesPerSegment
      val endRow = Math.min((segmentIndex + 1) * linesPerSegment - 1, totalRows - 1).toInt

      println(s"Processing segment $segmentIndex: rows $startRow to $endRow")

      val indexedDF = inputDF.withColumn("index", monotonically_increasing_id())
      val segmentDF = indexedDF.filter(col("index").between(startRow, endRow)).drop("index")

      // Convert DataFrame to JSON strings and send to Kafka


      segmentDF.toJSON.foreachPartition { (partition: Iterator[String]) =>
        val producer = new KafkaProducer[String, String](kafkaProps)
        partition.foreach { json: String =>
          val record = new ProducerRecord[String, String](topicName, json)
          producer.send(record)
        }
        producer.close()
      }


      Thread.sleep(intervalSeconds * 1000)
    }
  }
}
