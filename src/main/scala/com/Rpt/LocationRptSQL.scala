package com.Rpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

object LocationRptSQL {
  def main (args: Array[String]): Unit = {
    // 判断路径
    if(args.length != 1){
      println("目录错误！！")
      sys.exit()
    }
    // 创建输入输出路径
    val Array(inputPath) = args

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    // 创建执行入口
   val spark =  SparkSession
     .builder().config("spark.sql.parquet.compression.codec","snappy")
     .config(conf)
     .getOrCreate()

    val df: DataFrame = spark.read.parquet(inputPath)

    df.createOrReplaceTempView("log")
      // 生成临时表
    val res = spark.sql ("select provincename,cityname," +
      "sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ysSum," +
      "sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yxSum," +
      "sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adSum," +
      "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cySum," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess," +
      "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) shows," +
      "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) clicks," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspcost," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspapy " +
      "from log group by provincename,cityname")


    res.show()

    spark.stop()
  }
}
