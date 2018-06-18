package com.wqj.spark.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/18 15:23
  * @Description: 因为某些rdd十分的重要,怕数据丢失,故保存dfs中做备份  先first再second再checkpoint
  */
class CheckPoint {
  val conf =new SparkConf().setAppName("CheckPoint").setMaster("local")
  val sc =new SparkContext(conf)
  sc.setCheckpointDir("/spark/checkpoint/ck2018-6-18")
  val first=sc.textFile("/spark/tmp/word.txt").flatMap(_.split("/n")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false)
  val second=first.cache()
  val checkrdd= first.checkpoint()

}
