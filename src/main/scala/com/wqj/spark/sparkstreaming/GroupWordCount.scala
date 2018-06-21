package com.wqj.spark.sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.tools.scalap.scalax.rules.scalasig.ScalaSigEntryParsers.S

/**
  * @Auther: wqj
  * @Date: 2018/6/20 18:22
  * @Description:
  */
object GroupWordCount {



  //分好组的数据
  val func =(iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
  }
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("GroupWordCount").setMaster("local[2]");
    val sc=new SparkContext(conf)
    //使用updateStateByKey 必须设置checkpoint
    sc.setCheckpointDir("e://checkpoint")

    //后面的数 据为延迟数
    val ssc =new StreamingContext(sc,Seconds(5))
    //接收数据
    val ds=ssc.socketTextStream("master",7777)



    val result=ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(func,new HashPartitioner(sc.defaultMinPartitions),true)
    result.print()
    ssc.start();
    ssc.awaitTermination();
  }

}
