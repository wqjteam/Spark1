package com.wqj.spark.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/16 18:40
  * @Description:
  */
object ForeachDemo {
  def main(args: Array[String]): Unit = {
    //local[2]代表两个线程
    val conf =new SparkConf().setAppName("ForeachDemo").setMaster("local")
    val sc =new SparkContext(conf)
    val rdd1 =sc.parallelize(List(1,2,3,4,5,6,7,8,9))
    rdd1.foreach(x=>print(_))

    //业务上可以添加至数据库中
    sc.stop()
  }

}
