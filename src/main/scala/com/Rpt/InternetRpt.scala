package com.Rpt

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object InternetRpt {
  def main (args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
   val spark = SparkSession.builder().config(conf).getOrCreate()
    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 将数据进行处理，统计各个指标
    val tups = df.rdd.map(row => {

      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")

      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key值是地域的省市
     val networkmannername = row.getAs[String]("networkmannername")

      // 创建三个对应的方法处理九个指标
      //原始请求数，有效请求，广告请求
      val s = RptUtils.request(requestmode, processnode)
      // 展示数，点击数
      val a = RptUtils.click(requestmode, iseffective)
      // 参与竞价数，竞价成功数，广告消费，广告成本
      val d = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

      (networkmannername, s ++ a ++ d)
    })
    val sumed= tups.reduceByKey((list1, list2) => list1.zip(list2).map(x => x._2 + x._1))
    println(sumed.collect.toBuffer)

    // 如果想要存入mysql的话，需要使用foreachPartition
    // 需要自己写一个连接池
    spark.stop()
  }
}
