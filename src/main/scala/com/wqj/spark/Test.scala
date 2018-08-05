package com.wqj.spark

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable.ArrayBuffer

/**
  * @Auther: wqj
  * @Date: 2018/6/17 10:49
  * @Description:
  */

object Test extends App {
  val str="{\"id\":\"1\",\"name\":\"zs\"}"
  val map=new ObjectMapper()
  val student: Student = map.readValue(str,classOf[Student])
  print(student.toString)
//  var a=ArrayBuffer[(Int,Int)]()
//  a+=((1,1))
//  println(a.toBuffer)
//
//  val c=1
}
