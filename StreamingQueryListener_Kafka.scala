// Databricks notebook source
// MAGIC %md
// MAGIC #Streaming Query Listener that send query progress information to Kafka 
// MAGIC 
// MAGIC Steps:
// MAGIC 1. Modify Kafka configs
// MAGIC 2. Execute this notebook in a cell at the begining of your streaming job as following: `%run /Users/workingDir/NotebookLocation/StreamingQueryListener_Kafka`
// MAGIC 
// MAGIC 
// MAGIC ## Extends Streaming Query Listener 
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#reporting-metrics-programmatically-using-asynchronous-apis

// COMMAND ----------

// val kafka_bootstrap_servers_tls = "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094"
// val kafka_bootstrap_servers_plaintext = "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9092,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9092" 

// COMMAND ----------


// # Full username, e.g. "aaron.binns@databricks.com"
// val username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user")

val topic = "christopher_chalcraft_oetrta_kafka_test"

// COMMAND ----------

// DBTITLE 1,Create Query Listener 
import org.apache.spark.sql.streaming._
import StreamingQueryListener._
// import org.apache.kafka.clients.producer._
import java.util.Properties
import kafkashaded.org.apache.kafka.clients.producer._

class kafkaListener(topic:String, servers: String) extends StreamingQueryListener {
  
    // Kafka Properties
    private val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", servers)
    kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  
    // Unique ID for message sent to Kafka topic 
    private def uuid = java.util.UUID.randomUUID.toString

    // Create Kafka Producer
    val producer = new KafkaProducer[String, String](kafkaProperties)

    // Modify StreamingQueryListener Methods 
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    }
    // Send Query Progress metrics to Kafka 
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      producer
      .send(new ProducerRecord(topic, uuid, event.progress.json))
    }
  }

// val kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-plaintext" )
val kafka_bootstrap_servers_plaintext = "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9092,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9092" 
// val topic = "ssme_monitoring"

val streamingListener = new kafkaListener(topic, kafka_bootstrap_servers_plaintext)

// COMMAND ----------

// DBTITLE 1,Add to Query Listener
spark.streams.addListener(streamingListener)
