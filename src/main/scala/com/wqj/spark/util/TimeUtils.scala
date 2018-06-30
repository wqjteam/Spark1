package com.wqj.spark.util

import java.text.SimpleDateFormat

/**
  * @Auther: wqj
  * @Date: 2018/6/30 17:23
  * @Description:
  */
object TimeUtils {

  def DateStringToLong(dateString:String):Long={

    val sdf=new SimpleDateFormat("yyyy/mm/dd HH:MM:ss")
    sdf.parse(dateString).getTime
  }
}
