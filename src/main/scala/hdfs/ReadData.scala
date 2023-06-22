package hdfs

import org.apache.spark.sql.SparkSession

/**
 * Created by linhnm on June 2023
 */

object ReadData {
  val hdfsMaster = "hdfs://94.10.10.11:9000"
  val userDataPath = "/fenio/user-data.csv"

  def main(args: Array[String]): Unit = {
    val hdfsMaster = "hdfs://94.10.10.11:9000"
//    val userDataPath = "/dir/hadoop/hello_world.txt"
    val userDataPath = "/fenio/user-data.csv"

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HDFS")
      .getOrCreate()

    val dfCSV = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("header", "true")
      .csv(hdfsMaster + userDataPath)

    dfCSV.show()
    dfCSV.printSchema()

//    val df = spark.read.text(hdfsMaster + userDataPath)

//    df.show()
  }
}
