package com.wqj.spark.base

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/17 00:29
  * @Description:
  */
object SubjectDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //将其拆分成元祖(url,1)
    //可以加fifter进行时间限制
    val result1 = sc.textFile("d://itcast.log").map(lines => {
      val f = lines.split("\t")
      (f(1), 1)
    }).reduceByKey(_ + _)
    //组装成(host,url,num)
    val result2 = result1.map(x => {
      (new URL(x._1).getHost, x._1, x._2)
    })
    //将list中数据进行排序去前三,这里排序可能出问题
    //  val result3 = result2.groupBy(_._1).mapValues(x => {
    //    x.toList.sortBy(_._3).reverse.take(2)
    //  })

    //新方法 将各个学科拆分出出来
    //  result2.
    val netresult3=result2.filter(_._1=="net.itcast.cn").sortBy(_._3,false).take(2)
    val javaresult3=result2.filter(_._1=="java.itcast.cn").sortBy(_._3,false).take(2)
    //
    println(netresult3.toBuffer)
    println(javaresult3.toBuffer)


  }

}
