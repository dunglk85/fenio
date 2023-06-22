package com.linhnm

import org.apache.spark.sql.SparkSession
import streaming.Streaming

/**
 * Created by linhnm on June 2023
 */

object Main {
  def main(args: Array[String]): Unit = {
    println("hello dai ca Linh")

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    Streaming.readStreamKafka(spark);
  }

}
