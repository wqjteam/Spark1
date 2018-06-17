package com.wqj.spark.partition

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @Auther: wqj
  * @Date: 2018/6/17 09:55
  * @Description:
  */
object PartitionIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PartitionIndexDemo").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 12, 34, 56, 76, 798, 898, 10), 3)
    //    println(rdd1.partitions.size)
    val rdd2=rdd1.mapPartitionsWithIndex((a, itear) => {
      val arr = ArrayBuffer[(Int,Int)]()
      while (itear.hasNext) {
        arr +=((a, itear.next()))
      }
      arr.iterator
    })

    rdd2.foreach(x=>{
      println("结果为"+x )
    })
  }
}
