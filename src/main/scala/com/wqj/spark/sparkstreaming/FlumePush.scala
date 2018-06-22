package com.wqj.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * @Auther: wqj
  * @Date: 2018/6/21 14:55
  * @Description:
  */
object FlumePush {
  def main(args: Array[String]): Unit = {
    //分好组的数据
    val func =(iter: Iterator[(String, Seq[Int], Option[Int])]) => {
      //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
      //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
      //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
      iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
    }

    val conf = new SparkConf().setAppName("FlumePush").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //推送方式: flume向spark中推送数据
    val flumeStream = FlumeUtils.createStream(ssc, "192.168.8.149", 7777)
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
