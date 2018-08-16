package com.wqj.spark.base

import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/8/15 20:57
  * @Description:
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "300")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .appName("MergeParquetFileHoulyJob")
//      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()
    for(x <- 0 to 20){
      print(x)
    }
//    val rdd1 = spark.sparkContext.parallelize(Array("a", "a", "b", "c", "d", "f", "t", "g", "f", "g", "f"), 3)
//    val rdd2 = rdd1.map(x => {
//      (x, 1)
//    }).reduceByKey((x, y) => {
//      x + y
//    })
//
//    val rdd3 = rdd1.map(x => {
//      (x, 1)
//    }).groupByKey()
//    rdd2.saveAsTextFile("/user/result/2")
//    rdd3.saveAsTextFile("/user/result/3")
//    println("rdd2"+rdd2.toBuffer)
//    println("rdd3"+rdd3.toBuffer)
  }
}
