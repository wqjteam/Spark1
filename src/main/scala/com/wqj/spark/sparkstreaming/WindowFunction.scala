package com.wqj.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/22 18:34
  * @Description:
  */
object WindowFunction {
  def main(args: Array[String]) {
    //优雅停机
    //.set("spark.streaming.stopGracefullyOnShutdown","true")
    val conf = new SparkConf().setAppName("WindowOpts").set("spark.streaming.stopGracefullyOnShutdown","true").setMaster("local[2]")
    //毫秒数 更加精细
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    val lines = ssc.socketTextStream("master", 7777)
    val pairs = lines.flatMap(_.split(" ")).map((_, 1))
    //窗口函数是记录一段时间内的数据叠加
    //第一个参数Seconds(15)是时间长短
    //第一个参数Seconds(10)是滑动时间间隔  滑动时间间隔  和时间长短间隔 只和必须是 Milliseconds(5000) 倍数

    //时间框框是15  每次滑动10各单位,只计算 属于自己窗口的数据, 不进行累加,如果是全新的话 建议相同
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(15), Seconds(10))
    //Map((hello, 5), (jerry, 2), (kitty, 3))
    windowedWordCounts.print()
    //    val a = windowedWordCounts.map(_._2).reduce(_+_)
    //    a.foreachRDD(rdd => {
    //      println(rdd.take(0))
    //    })
    //    a.print()
    //    //windowedWordCounts.map(t => (t._1, t._2.toDouble / a.toD))
    //    windowedWordCounts.print()
    //    //result.print()
    ssc.start()

    ssc.awaitTermination()
    scala.sys.addShutdownHook({
      ssc.stop(true, true
      )
    })
  }
}
