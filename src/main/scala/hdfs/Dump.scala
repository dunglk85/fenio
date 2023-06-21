package hdfs

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random

/**
 * Created by linhnm on June 2023
 */

object Dump {
  def main(args: Array[String]): Unit = {

    val hdfsMaster = "hdfs://94.10.10.11:9000"
    val userDataPath = "/fenio/user-data.csv"

    val NUM_CUS = 10
    val rand = new Random()
    val startYear = 1960
    val endYear = 2000
    val range = endYear - startYear

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HDFS")
      .getOrCreate()

    val schema = List(
      StructField("customer_id", IntegerType, nullable = true),
      StructField("gender", IntegerType, nullable = true),
      StructField("birth_year", IntegerType, nullable = true)
    )

    var seq = Seq(Row(0, 1, 1994))
    for (i <- 1 until NUM_CUS) {
      val gender = if (rand.nextFloat() < 0.5) {1} else {0}
      seq = seq :+ Row(i, gender, startYear + rand.nextInt(range + 1))
    }

    val df = spark.createDataFrame(spark.sparkContext.parallelize(seq), StructType(schema))

    df.show()

    df.write.mode(SaveMode.Overwrite).option("header", "true").csv(hdfsMaster + userDataPath)

    val dfCSV = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("header", "true")
      .csv(hdfsMaster + userDataPath)

    dfCSV.show()

//    val dfParquet = spark.read.parquet(hdfsMaster + userDataPath)

//    dfParquet.show()
  }
}
