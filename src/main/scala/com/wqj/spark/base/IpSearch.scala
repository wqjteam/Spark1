package com.wqj.spark.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/17 17:48
  * @Description: IP查找
  */
object IpSearchDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("coustomsort").setMaster("local[2]")

    val sc = new SparkContext(conf)

  }
}
