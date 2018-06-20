package com.wqj.spark.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/20 16:57
  * @Description:
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]");
    val sc=new SparkContext(conf)
    //后面的数 据为延迟数
    val ssc =new StreamingContext(sc,Seconds(5))
    //接收数据
    val ds=ssc.socketTextStream("master",7777)

    val result=ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
//    val result2=result.mapPartitions(x=>{
//      x.map(x=>{
//          println(x._1+"|"+x._2)
//      })
//      x
//    })
    ssc.start();
    ssc.awaitTermination();
    result.print()
  }
}
