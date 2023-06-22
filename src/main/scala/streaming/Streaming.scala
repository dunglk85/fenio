package streaming

import dto.KafkaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Created by linhnm on June 2023
 */

object Streaming {
  def readStreamKafka(spark: SparkSession): Unit = {

    val hdfsMaster = "hdfs://94.10.10.11:9000"
    val userDataPath = "/fenio/user-data.csv"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConf.bootstrapServer)
      .option("subscribe", KafkaConf.topic)
      .option("startingOffsets", "earliest")
      .option("kafka.group.id", KafkaConf.groupId)
      .load()

    val userData = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("header", "true")
      .csv(hdfsMaster + userDataPath)

    //    df.printSchema()

    val customerLogStringDf = df.selectExpr("CAST(value AS STRING)");

    val schema = new StructType()
      .add("customer_id", IntegerType)
      .add("source", IntegerType)
      .add("target", IntegerType);

    val customerLog = customerLogStringDf.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    val customerDetail = customerLog
      .join(userData,
        customerLog("customer_id") === userData("id"), "inner")

    val maleCustomer = customerDetail.filter(customerDetail("gender") === 1)

//    val testFilterCustomerLog = customerLog.filter(customerLog("customer_id") % 2 === 0)

    maleCustomer.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
