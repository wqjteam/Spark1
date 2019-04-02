package com.wqj.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/9/11 17:52
  * @Description:
  */
object LogTest2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogTest2")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    val log = org.apache.log4j.LogManager.getLogger("ParquetReader")
    val rdd1: RDD[String] = sc.parallelize(Array("1", "2", "3", "4", "5"),2)
    while (true) {
      log.error("test2的数据****************************" + rdd1.collect().toBuffer + "**********************************")
      Thread.sleep(1000)
    }
  }
}
