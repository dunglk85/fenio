package streaming

import dto.KafkaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Created by linhnm on June 2023
 */

object Streaming {
  def readStreamKafka(spark: SparkSession): Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KafkaConf.topic)
      .option("kafka.group.id", KafkaConf.groupId)
      .load();


    df.printSchema();

    val customerLogStringDf = df.selectExpr("CAST(value AS STRING)");

    val schema = new StructType()
      .add("customer_id", IntegerType)
      .add("source", IntegerType)
      .add("target", IntegerType);

    val customerLog = customerLogStringDf.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    customerLog.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
