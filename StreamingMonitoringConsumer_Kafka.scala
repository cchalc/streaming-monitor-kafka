// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #Structure Streaming Kafka Consumer
// MAGIC 
// MAGIC Collect all the query progress logs from your kakfa topic to Delta Lake for further analysis/monitoring. 
// MAGIC 
// MAGIC - This notebook contains predefined schema/parsing for query progress (Feel free to modify/add)

// COMMAND ----------

// DBTITLE 1,Kafka Consumer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Kafka Configs
val startingOffsets = "earliest"
val kafka_bootstrap_servers_tls       = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-tls"       )
val kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-plaintext" )
val topic = "ssme_monitoring"

// Streaming duration Log Schema 
val durSchema = new StructType()
                            .add("addBatch", StringType)
                            .add("getBatch", StringType)
                            .add("latestOffset",StringType)
                            .add("queryPlanning",StringType)
                            .add("triggerExecution",StringType)
                            .add("walCommit",StringType)

// Streaming Query Progress Log Schema 
val structureSchema = new StructType()
                            .add("id", StringType)
                            .add("runId", StringType)
                            .add("name",StringType)
                            .add("timestamp",StringType)
                            .add("batchId",StringType)
                            .add("numInputRows",StringType)
                            .add("inputRowsPerSecond",StringType)
                            .add("processedRowsPerSecond",StringType)
                            .add("durationMs",durSchema)
                            .add("stateOperators",ArrayType(StringType))
                            .add("sources",ArrayType(StringType))
                            .add("sink",StringType)

// Read from Kafka 
val inputStream = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load())

// Map values 
val mapInputStream = inputStream
.select(//col("key").cast("string").alias("eventId"), 
        from_json(col("value").cast(StringType), structureSchema).alias("data"))
.select("data.*")
// display(mapInputStream, "monitoring_stream_1")

// COMMAND ----------

// DBTITLE 1,Delta Sink 
import org.apache.spark.sql.streaming.Trigger

val checkpoint = "/User/path"
// val checkpoint = "/Users/hector.camarena@databricks.com/checkpoint/monitoring_tmp_cp"

val processingTime = "10 seconds"
val tableSink = "database.table"
val queryName = "streaming_monitoring"


val streamSync = (mapInputStream.writeStream
                  .outputMode("append")
                  .format("delta")
                  .queryName(queryName)
                  .trigger(Trigger.ProcessingTime(processingTime))
                  .option("checkpointLocation", checkpoint)
                  .table(tableSink))

// COMMAND ----------

// DBTITLE 1,Query Streaming Metric Logs
// MAGIC %sql 
// MAGIC 
// MAGIC select * from <database.table>

// COMMAND ----------


