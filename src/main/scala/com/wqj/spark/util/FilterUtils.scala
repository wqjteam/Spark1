package com.wqj.spark.util

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat




/**
  * @Auther: wqj
  * @Date: 2018/6/30 16:45
  * @Description:
  */
object FilterUtils {

  val sdf=FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")
  def filterByTime(fields:Array[String],startTime:Long,endTime: Long):Boolean={
//val sdf=new SimpleDateFormat("yyyy年MM月dd日,E,HH:MM:ss")
    val time=sdf.parse(fields(1)).getTime
    if (time<=endTime&&time>startTime){
      true
    }else{
      false
    }

  }


  //计算次日留存用户
  def filterByTypeAndTime(fields: Array[String], eventType: String, beginTime: Long, endTime: Long): Boolean = {
    val _type = fields(0)
    val _time = fields(1)
    val logTime = sdf.parse(_time).getTime
    eventType == _type && logTime >= beginTime && logTime < endTime
  }

}
