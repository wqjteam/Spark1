package com.wqj.spark.sparkstreaming

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.time.Second

/**
  * @Auther: wqj
  * @Date: 2018/6/21 14:56
  * @Description:
  */
object FlumePoll {
  def main(args: Array[String]): Unit = {
    //分好组的数据
    val func =(iter: Iterator[(String, Seq[Int], Option[Int])]) => {
      //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
      //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
      //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
      iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
    }

    val conf = new SparkConf().setAppName("FlumePoll").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //推送方式: spark向flume中拉取送数据
    val address = Seq(new InetSocketAddress("192.168.8.149", 7777))
    //StorageLevel.MEMORY_AND_DISK 存储级别
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK )
    //flume 中 只有通过event.getBody() 才能拿到数据
    val words = flumeStream.flatMap(x => {
      new String(x.event.getBody().array()).split(" ").map((_,1))
    })
    val result= words.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
    //words.updateStateByKey()
  }
}
