package com.wqj.spark.game

import java.util.Calendar

import com.wqj.spark.common.EventType
import com.wqj.spark.util.{FilterUtils, TimeUtils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/30 15:51
  * @Description:
  */
object GameKpi {


  def main(args: Array[String]): Unit = {
    val date = TimeUtils.DateStringToLong("2016-02-01 00:00:00")

    val conf = new SparkConf().setAppName("GameKpi").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("D:/input/GameLog.txt").map(_.split("\\|")).cache()
    //筛选日志之后的数据
    val cacheDate = text.filter(x =>
      FilterUtils.filterByTime(x, TimeUtils.DateStringToLong("2016-02-01 00:00:00"), TimeUtils.DateStringToLong("2016-02-02 00:00:00"))).cache()

    //日新增用户,register只会有一次可以直接count
    val dnu = cacheDate.filter(x => {
      x(0).equals(EventType.REGISTER)
    }).count()
    println("新增用户"+dnu)

    //计算日活跃用户
    val drn = cacheDate.filter(x => {
      (!x(0).equals(EventType.REGISTER) && !x(0).equals(EventType.REGISTER))
    }).map((_ (3))).distinct().count()

    println("日活跃用户"+dnu)



    //次日留存 昨天的新注册的用户在今天还剩多少
    val beforenumber=cacheDate.filter(x=>FilterUtils.filterByTypeAndTime(x,EventType.REGISTER,date,TimeUtils.DateAddOrDel(date,1)))
      .map((_(3)->1))
    val todaynumber=text.filter(x=>FilterUtils.filterByTypeAndTime(x,EventType.LOGIN,TimeUtils.DateAddOrDel(date,1),TimeUtils.DateAddOrDel(date,2)))
        .map((_(3)->1)).distinct()
    val ci=beforenumber.join(todaynumber)

    val cl=ci.count() / beforenumber.count()
    println("次日留存率"+cl)
  }
}
