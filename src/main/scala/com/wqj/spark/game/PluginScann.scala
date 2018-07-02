package com.wqj.spark.game

import java.util

import com.wqj.spark.util.{RedisUtil, TimeUtils}
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
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    sc.setCheckpointDir("e://checkpoint")
    val map = Map(("test" -> 3));
    //    val kafkaParams = Map[String, String](
    //      "zookeeper.connect" -> "kafka:2181",
    //      "group.id" -> "local"
    //
    //      //      ,"auto.offset.reset" -> "smallest"
    //    )
    val stream = KafkaUtils.createStream(ssc, "kafka:2181", "local", map, StorageLevel.MEMORY_AND_DISK_SER)
    //_.1为patitioner的下标_._2为partitioner中的数据,发现第一个为null
    //@return DStream of (Kafka message key, Kafka message value)
    val lines = stream.map(x => {
      //      println(x._1, x._2)
      x._2
    })
    val qxysData = lines.map(_.split("/t")).filter(x => {

      x(3).eq("11") && x(8).equals("强效药水")
    })

    //按照窗口函数分好组的数据
    val windowDate = qxysData.map(x => {
      (x(7), x(12))
    }).groupByKeyAndWindow(Seconds(30), Seconds(20))

    //排除网络问题,排除5次使用的
    val caledata = windowDate.filter(x => {
      x._2.size > 5
    })

    //平均使用药水时长
    val pjsysc = caledata.mapValues(x => {
      val list = x.toList.sorted
      val count = list.size
      val time = TimeUtils.DateStringToLong(list(0)) - TimeUtils.DateStringToLong(list(count - 1))
      count.toLong / time
    })

    //如果大于某个标准就是 开挂了
    val kaiguadata = pjsysc.filter(x => {
      x._2 < 1000
    })
    kaiguadata.foreachRDD(x=>{
      x.foreachPartition(y=>{
        //一个循环开始
        val jedis=RedisUtil.getConnection()
        jedis.select(3);
        y.foreach(z=>{
          print("进入到方法中2")
          jedis.set(z._1,z._2.toString)
        })
        //一个循环完毕后 关了连接
        jedis.close()
      })
    })

    kaiguadata.print()
    ssc.start()
    ssc.awaitTermination()
    print("11")
  }

}
