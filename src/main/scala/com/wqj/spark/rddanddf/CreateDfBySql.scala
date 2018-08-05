package com.wqj.spark.rddanddf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

/**
  * @Auther: wqj
  * @Date: 2018/7/22 11:01
  * @Description:
  */
object CreateDfBySql {
  def main(args: Array[String]): Unit = {


    //  val conf = new SparkConf().setAppName("CreateDfBySql").setMaster("local[1]")
    //  val sc=new SparkContext()
    //  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //  //第二种, Spark中使用createDataFrame函数创建DataFrame
    //
    //  val schema = StructType(List(
    //    StructField("integer_column", IntegerType, nullable = false),
    //    StructField("string_column", StringType, nullable = true),
    //    StructField("date_column", DateType, nullable = true)
    //  ))
    //
    //  val rdd = sc.parallelize(Seq(
    //    Row(1, "First Value", java.sql.Date.valueOf("2010-01-01")),
    //    Row(2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
    //  ))
    //  val dataFrame: DataFrame = sqlContext.createDataFrame(rdd, schema)


    val session: SparkSession = SparkSession.builder().appName("CreateDfBySql").master("local[*]").getOrCreate()
    val schema = StructType(List(
      StructField("integer_column", IntegerType, nullable = false),
      StructField("string_column", StringType, nullable = true),
      StructField("date_column", DateType, nullable = true)
    ))

    val rdd = session.sparkContext.parallelize(Seq(
      Row(1, "First Value", java.sql.Date.valueOf("2010-01-01")),
      Row(2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
    ))
    val df: DataFrame = session.createDataFrame(rdd, schema)
    print(df.show())
  }
}
