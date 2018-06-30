package com.wqj.spark.util

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat




/**
  * @Auther: wqj
  * @Date: 2018/6/30 16:45
  * @Description:
  */
object FilterUtils {

  val sdf=FastDateFormat.getInstance("yyyy年mm月dd日,E,HH:MM:ss")
  def filterByTime(fields:Array[String],startTime:Long,endTime: Long):Boolean={
//val sdf=new SimpleDateFormat("yyyy年MM月dd日,E,HH:MM:ss")
    val time=sdf.parse(fields(1)).getTime
    if (time<=endTime&&time>startTime){
      true
    }else{
      false
    }

  }

}
