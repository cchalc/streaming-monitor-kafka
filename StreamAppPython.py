# Databricks notebook source
# DBTITLE 1,Parameters

dbutils.widgets.text("checkpoint", "/tmp/checkpoint/test", "Chekpoint Directory")
dbutils.widgets.text("tableName", "", "Table Name")
dbutils.widgets.text("queryName", "", "Query Name")
dbutils.widgets.text("triggerProcessTime", "10 seconds", "Trigger Process Time")
dbutils.widgets.text("maxFilesPerTrigger", "1", "Max Files/Trigger")

# COMMAND ----------

checkpoint = dbutils.widgets.get("checkpoint")
tableName = dbutils.widgets.get("tableName")
queryName = dbutils.widgets.get("queryName")
triggerProcessTime = dbutils.widgets.get("triggerProcessTime")
maxFilesPerTrigger = int(dbutils.widgets.get("maxFilesPerTrigger"))

# COMMAND ----------

maxFilesPerTrigger

# COMMAND ----------

dbutils.fs.rm(checkpoint, True)

# COMMAND ----------

# DBTITLE 1,Attach Query Listener 
# MAGIC %run ./StreamingQueryListener_Kafka

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.streams.listListeners

# COMMAND ----------

# DBTITLE 1,Get Schema from Json Files

myDf= spark.read.format("json").load("/databricks-datasets/iot-stream/data-device").limit(10)
myschema = myDf.schema

# COMMAND ----------

# DBTITLE 1,Input Stream
streamInput = (spark.readStream
              .format("json")
              .option("maxFilesPerTrigger", maxFilesPerTrigger)
              .schema(myschema)
              .load("/databricks-datasets/iot-stream/data-device"))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Stream Sink
streamSync = (streamInput.writeStream
                  .outputMode("append")
                  .format("delta")
                  .queryName(queryName)
                  .trigger(processingTime='2 seconds')
                  .option("checkpointLocation",checkpoint)
                  .table(tableName))
