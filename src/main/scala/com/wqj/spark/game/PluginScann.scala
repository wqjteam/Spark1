package com.wqj.spark.game

import java.util

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Auther: wqj
  * @Date: 2018/7/1 17:11
  * @Description:
  */
object PluginScann {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PluginScann").setMaster("local[3]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("auto.offset.reset", "smallest")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    sc.setCheckpointDir("e://checkpoint")
    val map= Map(("test"-> 3));
    val stream=KafkaUtils.createStream(ssc, "kafka:2181", "local",map,StorageLevel.MEMORY_AND_DISK_SER)
    //_.1为patitioner的下标_._2为partitioner中的数据
   //@return DStream of (Kafka message key, Kafka message value)
    val lines=stream.map(x=>{
      println("数据为key:"+x._1,"数据的value为:"+x._2)
      x._2
    })
    val beforeData = lines.map(_.split("/t"))
    val afterdate=beforeData.filter(f=>{
     val et= f(3)
      val item=f(8)
      et.equals("11")&&item.equals("强效太阳水")
    })
    afterdate.print()
    ssc.start()
    ssc.awaitTermination()
    print("11")
  }

}
