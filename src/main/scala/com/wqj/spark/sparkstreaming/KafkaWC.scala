package com.wqj.spark.sparkstreaming

import com.wqj.spark.util.RedisUtil
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * @Auther: wqj
  * @Date: 2018/6/22 11:51
  * @Description:
  */
object KafkaWC {
  //分好组的数据
  val func =(iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
  }

  def main(args: Array[String]): Unit = {
    val Array(zkQuorm, group, topics, numThreads) = args
    val conf = new SparkConf().setAppName("KafkaWC").set("spark.streaming.stopGracefullyOnShutdown","true").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //streaming需要指定地址
    ssc.checkpoint("e://checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //kafka可以从多个topic中获取数据
    val data = KafkaUtils.createStream(ssc, zkQuorm, group, topicMap)

    val words = data.map(_._2).flatMap(_.split(" "))

    val wordCounts = words.map((_,1)).updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultMinPartitions),true)

   // wordCounts.print()

    //准备将数据存入redis中
    wordCounts.foreachRDD(x=>{
      x.foreachPartition(y=>{
        val jedis=RedisUtil.pool.getResource
        jedis.select(3);
        y.foreach(z=>{
          print("进入到方法中2")
          jedis.set(z._1,z._2.toString)
        })
    })
    })
    wordCounts.print();
    ssc.start()

    ssc.awaitTermination()
  }
}
