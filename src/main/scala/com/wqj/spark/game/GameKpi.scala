package com.wqj.spark.game

import com.wqj.spark.util.{FilterUtils, TimeUtils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/30 15:51
  * @Description:
  */
object GameKpi {



  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("GameKpi").setMaster("local[3]")
    val sc=new SparkContext(conf)
    val text=sc.textFile("D:\\input\GameLog.txt").map(_.split("\\|"))
    //筛选日志之后的数据
    val cacheDate=text.filter(x=>FilterUtils.filterByTime(x,TimeUtils.DateStringToLong("2015/6/30 15:51:11"),TimeUtils.DateStringToLong("2018/6/30 15:51:11"))).cache()
  }
}
