package com.wqj.spark.sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/6/18 22:47
  * @Description:
  */
object SqlDemo {
  def main(args: Array[String]): Unit = {



  val conf = new SparkConf().setAppName("SqlDemo").setMaster("local[2]")
  val sc = new SparkContext(conf);
  val sqlContext = new SQLContext(sc)
  val rdd1 = sc.parallelize(Array(("zhangsan", 1), ("lisi", 2), ("wangwu", 3), ("zhaoliu", 4), ("liqi", 5)))
  val rdd2 = rdd1.map(x => {
    Person(x._1, x._2)
  })

  import sqlContext.implicits._

  val df1= rdd2.toDF()
  df1.registerTempTable("person_table")
    println(sqlContext.sql("select * from person_table ").show())
    println(sqlContext.sql("select * from person_table ").toJSON)
//  println(sqlContext.sql("select * from person_table order by age desc limit 2").show())
  }
}


case class Person(name: String, age: Int)