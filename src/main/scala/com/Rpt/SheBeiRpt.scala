package com.Rpt

import com.utils.RptUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SheBeiRpt {
  def main (args: Array[String]): Unit = {
    // 判断路径
    if(args.length != 2){
      println("路径错误，退出！！！")
      sys.exit()
    }
    // 创建输入输出路径
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val df: DataFrame = spark.read.parquet(inputPath)
    val tups = df.rdd.map(row =>{
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

      val devicetype = row.getAs[Int ]("devicetype")
      var s2 = ""
      if(devicetype ==1){
        s2 ="手机"
      }else if(devicetype == 2){
        s2 ="平板"
      }else{
        s2 ="其他"
      }

      // 处理 原始请求数，有效请求，广告请求
      val s = RptUtils.request(requestmode, processnode)
      // 展示数，点击数
      val a = RptUtils.click(requestmode, iseffective)
      // 参与竞价数，竞价成功数，广告消费，广告成本
      val d = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

      (s2, s ++ a ++ d)
    })
    val res: RDD[(String, List[Double])] = tups.reduceByKey((list1, list2)=> list1.zip(list2).map(x=> x._2+x._1))

    res.saveAsTextFile(outputPath)
    spark.stop()

  }
}
