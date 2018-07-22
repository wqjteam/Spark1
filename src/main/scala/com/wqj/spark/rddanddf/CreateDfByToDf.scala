package com.wqj.spark.rddanddf

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: wqj
  * @Date: 2018/7/22 10:03
  * @Description:
  */
case class Person(name: String, age: Int)
object CreateDfByToDf{
  def main(args: Array[String]): Unit = {

    //第一种 ,通过本地seq + toDF创建DataFrame示例
    val conf = new SparkConf().setAppName("CreateDfByToDf").setMaster("local[1]")
    val sc=new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Define the schema using a case class.
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // you can use custom classes that implement the Product interface.


    // Create an RDD of Person objects and register it as a table.
    val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => {Person(p(0), p(1).trim.toInt)}).toDF()
    people.registerTempTable("people")

    // 使用 sqlContext 执行 sql 语句.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // 注：sql()函数的执行结果也是DataFrame，支持各种常用的RDD操作.
    // The columns of a row in the result can be accessed by ordinal.
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)


    import sqlContext.implicits._
    val df = Seq(
      (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
      (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
    ).toDF("int_column", "string_column", "date_column")



  }
}
