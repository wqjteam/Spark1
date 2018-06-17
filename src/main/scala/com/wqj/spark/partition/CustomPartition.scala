package com.wqj.spark.partition

import org.apache.spark.{Partition, Partitioner}

import scala.collection.mutable

/**
  * @Auther: wqj
  * @Date: 2018/6/17 11:09
  * @Description:
  */
class CustomPartition(ins: Array[String]) extends Partitioner{


val parMap =new mutable.HashMap[String,Int]()
  var count =0
  for (url<-ins){
    parMap+=((url,count))
    count+=1
  }


  override def numPartitions = {
      ins.length
  }

  override def getPartition(key: Any) = {
      parMap.getOrElse(key.toString,0)
  }
}
