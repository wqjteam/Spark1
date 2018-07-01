package com.wqj.spark.util

import java.sql.Date
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

/**
  * @Auther: wqj
  * @Date: 2018/6/30 17:23
  * @Description:
  */
object TimeUtils {

  def DateStringToLong(dateString:String):Long={

    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(dateString).getTime
  }

  def DateStringToDate(dateString:String):util.Date={

    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(dateString)
  }

  def DateAddOrDel(date1:Long,num:Int):Long={
    val cale=Calendar.getInstance()
    cale.setTimeInMillis(date1)
    cale.add(Calendar.DATE,num )
    cale.getTime.getTime
  }
}
