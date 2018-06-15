package com.wqj.spark.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/15 14:26
  * @Description:
  */
object WordCount {


  def main(args: Array[String]): Unit = {
    //通向spark集群的入口
    val conf= new SparkConf().setAppName("WC")
    val sc=new SparkContext()

    sc.textFile("/spark/tmp/word.txt").flatMap(_.split("/n")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).saveAsTextFile("/spark/output/")
  }
}
