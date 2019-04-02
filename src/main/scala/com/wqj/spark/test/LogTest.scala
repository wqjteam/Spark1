package com.wqj.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.bson.types.ObjectId

/**
  * @Auther: wqj
  * @Date: 2018/9/11 17:52
  * @Description:
  */
object LogTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogTest")

      if(args.size==0){
        conf.setMaster("local[1]")
      }

    System.getProperty("logname")
    val sparkSession = SparkSession.builder
      .config(conf)
//      .enableHiveSupport()
      .getOrCreate()
    val log = org.apache.log4j.LogManager.getLogger("ParquetReader")
    for (i <- 0 until 1000){
      new ObjectId()

    }
//    val rdd1: RDD[String] = sparkSession.sparkContext.parallelize(Array("a", "b", "c", "d", "f"), 2)
//    while (true) {
//      log.error("test1的数据-------------------------------------" + rdd1.collect().toBuffer + "------------------------")
//      Thread.sleep(1000)
//    }
  }
}
