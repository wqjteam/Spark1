package com.wqj.spark.sparkstreaming

import com.wqj.spark.util.RedisUtil
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.ObjectMapper

/**
  * @Auther: wqj
  * @Date: 2018/8/9 15:02
  * @Description:
  */
case class Table(database: String, table: String, pk_id: Int)
case class Student(id: Int, name: String, age: Int)

object DriectKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DriectKafka")
      .setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf, Seconds(5))
    //kafka的属性
    val topics = Set("test")
    val groupId = "DriectKafka2"
    val brokers = "hadoop1:9092"
    //配置参数
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )
    //生成kafka管理
    val km = new KafkaManager(kafkaParams)

    val kafkaStreaming = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    kafkaStreaming.foreachRDD(x => {
      if (!x.isEmpty()) {
        dealMessage(x)
        km.updateZKOffsets(x)
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }


  def dealMessage(rdd: RDD[(String, String)]): Unit = {
    rdd.foreach(x=>{
     val mapper= new ObjectMapper()
      mapper.
      println(x._1+"|"+x._2)
    })
    rdd.foreachPartition(x => {
      val jedis=RedisUtil.pool.getResource
      jedis.select(3);
      println(x)
    })
    //    println(rdd.collect().toBuffer);
  }


}
