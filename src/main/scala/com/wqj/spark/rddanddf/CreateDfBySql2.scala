package com.wqj.spark.rddanddf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @Auther: wqj
  * @Date: 2018/7/22 11:01
  * @Description:
  */
object CreateDfBySql2 {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("CreateDfBySql").master("local[*]").getOrCreate()
    val schema = StructType(List(
      StructField("phonenumber", StringType, true),
      StructField("time", StringType, true),
      StructField("uuid", StringType, true),
      StructField("load", StringType, true)
    ))
    val rdd = session.sparkContext.textFile("input//basestation").map(x => {
      val strings: Array[String] = x.toString.split(",")
      Row(strings(0),strings(1),strings(2),strings(3))
    })

    val df: DataFrame = session.createDataFrame(rdd, schema)
//    df.select("phonenumber","time").write.json("output/json")

    val frame: DataFrame = session.read.json("output/json/part-00000-3f546ece-2d45-444c-95e8-9dc2e941b15a.json")
    print(frame.show())
  }
}
