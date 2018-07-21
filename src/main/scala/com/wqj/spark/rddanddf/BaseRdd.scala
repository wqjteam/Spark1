package com.wqj.spark.rddanddf

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/7/21 14:22
  * @Description:
  */
object BaseRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BaseRdd").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("input//basestation")
    val afterline = lines.map(x => {
      val splits = x.split(",")
      val mobile = splits(0)
      val mac = splits(2)
      val time =
        if (splits(3).equals("1")) {
          -splits(1).toLong
        } else {
          splits(1).toLong
        }
      ((mobile, mac), time)
    }
    )
    val value = afterline.reduceByKey(_ + _)
    val time = value.map(x => {
      //(uuid,(时间,手机号码))
      (x._1._1, (x._2, x._1._2))
    })
    val value1: RDD[(String, Iterable[(Long, String)])] = time.groupByKey()
    val twospace = value1.map(x => {
      val value2: Iterable[(Long, String)] = x._2
      val tuples: List[(Long, String)] = value2.toList.sortWith(_._1 > _._1).take(2)
      (x._1, tuples)
    }).flatMap(x => {
      x._2.map(b => {
        (b._2, (b._1, x._1))
      })
    })
    //获取基站的数据
    val lines2 = sc.textFile("input//location")
    val result2 = lines2.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(0), (strings(1), strings(2)))
    })
    val value3: RDD[(String, ((Long, String), (String, String)))] = twospace.join(result2)
    //
    val value4 = value3.map(x => {
      (x._2._1._2,x._2._2)
    })

    print(value4.groupByKey().collect().toBuffer)
  }

}
