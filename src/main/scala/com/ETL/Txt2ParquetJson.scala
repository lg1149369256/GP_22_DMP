package com.ETL

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Txt2ParquetJson {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val logs = spark.read.parquet("D://qianfeng/gp22Dmp/output-20190820").rdd
    val tupes = logs.map(line => {
      val provinceName = line.getString(24)
      val cityName = line.getString(25)
      ((provinceName, cityName), 1)
    })
    val sumed = tupes.reduceByKey(_+_)
    val res = sumed.map(t => {
      val ct1 = t._2
      val provinceName1 = t._1._1
      val cityName1 = t._1._2
      (ct1, provinceName1, cityName1)
    })
    import spark.implicits._
    val df: DataFrame = res.toDF("ct","provinceName","cityName")
    //df.show()

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    val url = "jdbc:mysql://hadoop01:3306/test?characterEncoding=UTF-8"
    df.write.jdbc(url,"aaaa",prop)

    sc.stop()
  }
}
