package com.wqj.spark.rddanddf

import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row, SparkSession}

/**
  * @Auther: wqj
  * @Date: 2018/7/22 11:01
  * @Description:
  */
object CreateDfBySql3 {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("CreateDfBySql").master("local[*]").getOrCreate()
    val schema = StructType(List(
      StructField("phonenumber", StringType, true),
      StructField("time", StringType, true),
      StructField("uuid", StringType, true),
      StructField("load", StringType, true),
      StructField("i", IntegerType, true)
    ))
    val schema2 = StructType(List(
      StructField("uuid", StringType, true),
      StructField("jingdu", StringType, true),
      StructField("weidu", StringType, true),
      StructField("xinhao", StringType, true)
//      StructField("xinhao3", StringType, true)
    ))
    var i = 0
    val rdd = session.sparkContext.textFile("input//basestation").map(x => {
      val strings: Array[String] = x.toString.split(",")
      i += 1
      Row(strings(0), strings(1), strings(2), strings(3), i)
    })
    val rdd2 = session.sparkContext.textFile("input//location").map(x => {
      val strings: Array[String] = x.toString.split(",")
      Row(strings(0), strings(1), strings(2), strings(3))
    })

    val df: DataFrame = session.createDataFrame(rdd, schema)
    val df2: DataFrame = session.createDataFrame(rdd2, schema2)
    //    print(df.filter("time >20160327181500").count())
    //    df.select(df("phonenumber").as("ph"), df("time"), df("i")).show()
    val frame: DataFrame = df.select(df("phonenumber").as("ph"), df("time"), df("i") + (2))
    //    val frame: DataFrame = session.read.json("output/json/part-00000-3f546ece-2d45-444c-95e8-9dc2e941b15a.json")
    //    print(frame.show())
    val dataFrame: DataFrame = df.join(df2,df("uuid").equalTo(df2("uuid")))
     print(dataFrame.groupBy(df("phonenumber"),df2("uuid")))
  }
}
