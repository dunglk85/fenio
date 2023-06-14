package com.linhnm

import org.apache.spark.sql.SparkSession

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

    //  val df = spark.read.csv("data/201508_trip_data.csv")
    //  df.show

    val bankDf = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ";", "header" -> "true")).csv("data/bank/bank.csv")
    bankDf.show()

  }

}
