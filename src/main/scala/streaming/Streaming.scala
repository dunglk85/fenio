package streaming

import dto.KafkaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Created by linhnm on June 2023
 */

object Streaming {
  def process(spark: SparkSession): Unit = {

    val hdfsMaster = "hdfs://94.10.10.11:9000"
    val userDataPath = "/fenio/user-data.csv"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConf.bootstrapServer)
      .option("subscribe", KafkaConf.logTopic)
      .option("startingOffsets", "earliest")
      .option("kafka.group.id", KafkaConf.groupId)
      .load()

    val userData = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("header", "true")
      .csv(hdfsMaster + userDataPath)

    //    df.printSchema()

    val customerLogStringDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

    val schema = new StructType()
      .add("customer_id", IntegerType)
      .add("source", IntegerType)
      .add("target", IntegerType);

    val customerDetail = customerLogStringDf
      .select(col("key").as("key"), from_json(col("value"), schema).as("data"))
      .select("key", "data.*").alias("customerLog")
      .join(userData, col("customerLog.customer_id") === userData("id"), "inner")
      .select(col("key"),
        to_json(struct("id", "gender", "birth_year", "customerLog.source", "customerLog.target")).alias("value"))
      .filter(col("gender") === 1)


    /*
    customerDetail
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

     */

    customerDetail
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .writeStream
          .trigger(Trigger.ProcessingTime("5 seconds"))
          .format("kafka")
          .option("kafka.bootstrap.servers", KafkaConf.bootstrapServer)
          .option("topic", KafkaConf.messageTopic)
          .option("checkpointLocation", "/tmp/checkpoint")
          .start()
          .awaitTermination()
  }
}
